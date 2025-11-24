"""
Airbyte setup script for PostgreSQL to Databricks data ingestion.
This script configures source, destination, and connection for the dvd_rental database.
"""

import requests
import json
import os
import sys
from typing import Dict, List, Optional


class AirbyteClient:
    """Client for interacting with Airbyte API."""
    
    def __init__(self, base_url: str, username: str = None, password: str = None, api_key: str = None, 
                 workspace_id: str = None, client_id: str = None, client_secret: str = None):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.api_key = api_key
        self.client_id = client_id
        self.client_secret = client_secret
        self.workspace_id = workspace_id
        self.session = requests.Session()
        self.access_token = None
        
    def _refresh_token(self) -> bool:
        """Refresh the access token using client credentials."""
        if not (self.client_id and self.client_secret):
            return False
        
        try:
            token_response = requests.post(
                f"{self.base_url.replace('/api/public', '/api')}/v1/applications/token",
                headers={"Content-Type": "application/json"},
                json={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                }
            )
            
            if token_response.status_code == 200:
                data = token_response.json()
                self.access_token = data.get("access_token")
                if self.access_token:
                    self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})
                    return True
        except Exception as e:
            print(f"⚠ Token refresh failed: {e}")
        return False
        
    def authenticate(self) -> None:
        """Authenticate with Airbyte API using Client Credentials or Access Token."""
        
        # Method 1: Use client_id and client_secret to get an access token
        if self.client_id and self.client_secret:
            if self._refresh_token():
                # Update base URL to use public API
                public_base = self.base_url.replace("/api", "/api/public")
                self.base_url = public_base
                
                # Verify by listing workspaces
                ws_response = self.session.get(f"{self.base_url}/v1/workspaces")
                if ws_response.status_code == 200:
                    workspaces = ws_response.json().get("data", [])
                    if workspaces and not self.workspace_id:
                        self.workspace_id = workspaces[0]["workspaceId"]
                    print("✓ Authenticated with Airbyte API using Client Credentials")
                    return
                else:
                    print(f"⚠ Workspace verification failed: {ws_response.status_code} - {ws_response.text}")
            else:
                print("⚠ Failed to get access token from client credentials")
        
        # Method 2: Use pre-obtained access token directly
        if self.api_key:
            try:
                # Public API uses /api/public/v1 and access token in Authorization header
                public_base = self.base_url.replace("/api", "/api/public")
                self.access_token = self.api_key
                self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})
                
                # Verify by listing workspaces
                response = self.session.get(f"{public_base}/v1/workspaces")
                
                if response.status_code == 200:
                    workspaces = response.json().get("data", [])
                    if workspaces and not self.workspace_id:
                        self.workspace_id = workspaces[0]["workspaceId"]
                    # Update base URL to use public API
                    self.base_url = public_base
                    print("✓ Authenticated with Airbyte API using Access Token")
                    return
                else:
                    print(f"⚠ Access token verification failed: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"⚠ Access token authentication failed: {e}, trying other methods...")
        
        # Try basic auth directly on admin API endpoints
        if self.username and self.password:
            self.session.auth = (self.username, self.password)
            
            try:
                response = self.session.post(
                    f"{self.base_url}/v1/workspaces/list",
                    headers=self.get_headers(include_auth=False),
                    json={}
                )
                
                if response.status_code == 200:
                    workspaces = response.json().get("workspaces", [])
                    if workspaces:
                        self.workspace_id = workspaces[0]["workspaceId"]
                    print("✓ Authenticated with Airbyte API using basic auth")
                    return
                elif response.status_code in (401, 403):
                    # Try login endpoints as fallback
                    login_payload = {
                        "email": self.username,
                        "password": self.password
                    }
                    
                    login_endpoints = [
                        f"{self.base_url}/v1/users/login",
                        f"{self.base_url}/v1/admins/login"
                    ]
                    
                    for endpoint in login_endpoints:
                        login_response = self.session.post(
                            endpoint,
                            headers=self.get_headers(include_auth=False),
                            json=login_payload
                        )
                        
                        if login_response.status_code == 200:
                            data = login_response.json()
                            token = (
                                data.get("accessToken")
                                or data.get("token")
                                or data.get("sessionToken")
                                or data.get("session_token")
                                or (data.get("tokens") or {}).get("access")
                                or (data.get("authorization") or {}).get("accessToken")
                            )
                            
                            if token:
                                self.access_token = token
                                self.session.auth = None  # Remove basic auth
                                self.session.headers.update({"Authorization": f"Bearer {token}"})
                            
                            if not self.workspace_id:
                                self.workspace_id = data.get("defaultWorkspaceId") or data.get("workspaceId")
                            
                            print("✓ Authenticated with Airbyte API via login endpoint")
                            return
                    
                    # If we have a workspace_id, we can try to proceed (but API calls may still fail)
                    if self.workspace_id:
                        print(f"⚠ Authentication failed ({response.status_code}), but workspace ID provided. Continuing...")
                        print("⚠ Note: API calls may fail without proper authentication. Consider using API keys.")
                        return
                    
                    raise Exception(f"Authentication failed: {response.status_code} - {response.text}")
                else:
                    response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                # If we have workspace_id, allow continuing (with warning)
                if self.workspace_id and e.response.status_code in (401, 403):
                    print(f"⚠ Authentication error ({e.response.status_code}), but workspace ID provided. Continuing...")
                    print("⚠ Note: API calls may fail without proper authentication. Consider using API keys.")
                    return
                raise Exception(f"Unable to authenticate with Airbyte API: {e}")
        
        # If no auth provided but workspace_id is set, allow continuing
        if self.workspace_id:
            print("⚠ No authentication provided, but workspace ID is set. Continuing...")
            print("⚠ Note: API calls may fail without proper authentication. Consider using API keys.")
            return
        
        raise Exception("No authentication method provided. Please provide either API key or username/password.")
        
    def get_headers(self, include_auth: bool = True, refresh_token: bool = True) -> Dict[str, str]:
        """Get headers for API requests. Optionally refresh token before request."""
        headers = {
            "Content-Type": "application/json"
        }
    
        # Refresh token if using client credentials (tokens expire after 3 minutes)
        if refresh_token and self.client_id and self.client_secret:
            self._refresh_token()
        
        # If we have a token, use Bearer auth; otherwise rely on session.auth (basic auth)
        if include_auth and self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        
        return headers
    
    def get_workspace(self, workspace_id: Optional[str] = None) -> str:
        """Get the workspace ID. If provided, use it; otherwise fetch the first one."""
        if workspace_id:
            self.workspace_id = workspace_id
            print(f"✓ Using provided workspace: {self.workspace_id}")
            return self.workspace_id
        
        if self.workspace_id:
            print(f"✓ Using cached workspace: {self.workspace_id}")
            return self.workspace_id
        
        response = self.session.post(
            f"{self.base_url}/v1/workspaces/list",
            headers=self.get_headers(),
            json={}
        )
        response.raise_for_status()
        workspaces = response.json().get("workspaces", [])
        
        if not workspaces:
            raise Exception("No workspaces found")
            
        self.workspace_id = workspaces[0]["workspaceId"]
        print(f"✓ Using workspace: {self.workspace_id}")
        return self.workspace_id
    
    def create_postgres_source(self) -> str:
        """Create PostgreSQL source connector."""
        # Use hardcoded Postgres source definition ID (standard across Airbyte)
        # This avoids the forbidden source_definitions endpoint
        postgres_def = "decd338e-5647-4c0b-adf4-da0e75f5a750"
        
        source_config = {
            "name": "dvd_rental",
            "workspaceId": self.workspace_id,
            "definitionId": postgres_def,
            "configuration": {
                "host": "host.docker.internal",
                "port": 5433,
                "database": "dvd_rental",
                "username": "postgres",
                "password": "postgres",
                "ssl_mode": {
                    "mode": "disable"
                },
                "replication_method": {
                    "method": "Standard"
                },
                "tunnel_method": {
                    "tunnel_method": "NO_TUNNEL"
                }
            }
        }
        
        response = self.session.post(
            f"{self.base_url}/v1/sources",
            headers=self.get_headers(),
            json=source_config
        )
        response.raise_for_status()
        result = response.json()
        source_id = result.get("sourceId") or result.get("id")
        print(f"✓ Created PostgreSQL source: {source_id}")
        return source_id
    
    def create_databricks_destination(
        self,
        host: str,
        http_path: str,
        token: str,
        catalog: str,
        schema: str
    ) -> str:
        """Create Databricks destination connector."""
        # Use hardcoded Databricks destination definition ID (standard across Airbyte)
        databricks_def = "072d5540-f236-4294-ba7c-ade8fd918496"
        
        destination_config = {
            "name": "Databricks",
            "workspaceId": self.workspace_id,
            "definitionId": databricks_def,
            "configuration": {
                "accept_terms": True,
                "hostname": host,
                "http_path": http_path,
                "port": "443",
                "database": catalog,
                "schema": schema,
                "authentication": {
                    "auth_type": "BASIC",
                    "personal_access_token": token
                }
            }
        }
        
        response = self.session.post(
            f"{self.base_url}/v1/destinations",
            headers=self.get_headers(),
            json=destination_config
        )
        response.raise_for_status()
        result = response.json()
        destination_id = result.get("destinationId") or result.get("id")
        print(f"✓ Created Databricks destination: {destination_id}")
        return destination_id
    
    def get_source_schema(self, source_id: str) -> Dict:
        """Discover schema from source."""
        print("⟳ Discovering schema from PostgreSQL source...")
        
        # Try using the discover endpoint with workspaceId
        response = self.session.post(
            f"{self.base_url}/v1/sources/discover",
            headers=self.get_headers(),
            json={
                "sourceId": source_id,
                "disable_cache": False
            }
        )
        response.raise_for_status()
        result = response.json()
        
        # The result might be nested differently depending on API version
        catalog = result.get("catalog", result.get("catalogDiff", {}).get("transforms", []))
        if isinstance(catalog, dict):
            streams = catalog.get("streams", [])
        else:
            # If catalog is a list, it might be the transforms array
            streams = []
            catalog = {"streams": streams}
        
        print(f"✓ Discovered {len(streams)} tables")
        return catalog
    
    def create_connection_with_streams(
        self,
        source_id: str,
        destination_id: str,
        catalog: Dict,
        primary_keys: Dict[str, List[str]] = None
    ) -> str:
        """Create connection between source and destination with configured streams."""
        
        # Configure all discovered streams
        configured_streams = []
        for stream in catalog.get("streams", []):
            stream_name = stream.get("name")
            namespace = stream.get("namespace", "public")
            
            # Determine primary key
            primary_key = []
            if primary_keys and stream_name in primary_keys:
                # Use provided primary keys
                primary_key = [[key] for key in primary_keys[stream_name]]
            elif stream.get("sourceDefinedPrimaryKey"):
                # Use source-defined primary keys
                primary_key = stream.get("sourceDefinedPrimaryKey", [])
            
            # Skip views (tables without primary keys) for simplicity
            # You can enable them by removing this check
            if not primary_key:
                print(f"  Skipping {stream_name} (no primary key - likely a view)")
                continue
            
            # Configure stream with Full Refresh | Overwrite
            configured_stream = {
                "stream": {
                    "name": stream_name,
                    "namespace": namespace,
                    "jsonSchema": stream.get("jsonSchema", {}),
                    "supportedSyncModes": stream.get("supportedSyncModes", ["full_refresh"]),
                    "sourceDefinedPrimaryKey": primary_key,
                },
                "config": {
                    "selected": True,
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                    "primaryKey": primary_key,
                    "aliasName": stream_name
                }
            }
            
            configured_streams.append(configured_stream)
        
        print(f"✓ Configured {len(configured_streams)} streams")
        
        connection_config = {
            "name": "dvd_rental → Databricks",
            "sourceId": source_id,
            "destinationId": destination_id,
            "configurations": {
                "streams": configured_streams
            },
            "schedule": {
                "scheduleType": "manual"
            },
            "status": "active",
            "dataResidency": "auto"
        }
        
        response = self.session.post(
            f"{self.base_url}/v1/connections",
            headers=self.get_headers(),
            json=connection_config
        )
        response.raise_for_status()
        result = response.json()
        connection_id = result.get("connectionId") or result.get("id")
        print(f"✓ Created connection: {connection_id}")
        return connection_id
    
    def update_connection_streams(
        self,
        connection_id: str,
        catalog: Dict,
        primary_keys: Dict[str, List[str]] = None
    ) -> None:
        """Update an existing connection with configured streams."""
        
        # Configure all discovered streams
        configured_streams = []
        for stream in catalog.get("streams", []):
            stream_name = stream.get("name")
            namespace = stream.get("namespace", "public")
            
            # Determine primary key
            primary_key = []
            if primary_keys and stream_name in primary_keys:
                # Use provided primary keys
                primary_key = [[key] for key in primary_keys[stream_name]]
            elif stream.get("sourceDefinedPrimaryKey"):
                # Use source-defined primary keys
                primary_key = stream.get("sourceDefinedPrimaryKey", [])
            
            # Skip views (tables without primary keys)
            if not primary_key:
                print(f"  Skipping {stream_name} (no primary key - likely a view)")
                continue
            
            # Configure stream with Full Refresh | Overwrite
            configured_stream = {
                "stream": {
                    "name": stream_name,
                    "namespace": namespace,
                    "jsonSchema": stream.get("jsonSchema", {}),
                    "supportedSyncModes": stream.get("supportedSyncModes", ["full_refresh"]),
                    "sourceDefinedPrimaryKey": primary_key,
                },
                "config": {
                    "selected": True,
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                    "primaryKey": primary_key,
                    "aliasName": stream_name
                }
            }
            
            configured_streams.append(configured_stream)
        
        print(f"✓ Configured {len(configured_streams)} streams")
        
        # Update connection with streams
        update_config = {
            "configurations": {
                "streams": configured_streams
            }
        }
        
        response = self.session.patch(
            f"{self.base_url}/v1/connections/{connection_id}",
            headers=self.get_headers(),
            json=update_config
        )
        response.raise_for_status()
        print(f"✓ Updated connection with streams: {connection_id}")
    
    def trigger_sync(self, connection_id: str) -> str:
        """Trigger a manual sync for the connection using the Jobs API."""
        # According to https://reference.airbyte.com/reference/createjob
        response = self.session.post(
            f"{self.base_url}/v1/jobs",
            headers=self.get_headers(),
            json={
                "connectionId": connection_id,
                "jobType": "sync"
            }
        )
        response.raise_for_status()
        result = response.json()
        job_id = result.get("jobId") or result.get("job", {}).get("id")
        print(f"✓ Triggered sync job: {job_id}")
        return job_id


def main():
    """Main setup function."""
    
    # Configuration
    AIRBYTE_URL = os.getenv("AIRBYTE_URL", "http://localhost:8000/api")
    CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
    CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
    
    # Alternative: Use a pre-obtained access token directly
    ACCESS_TOKEN = os.getenv("AIRBYTE_ACCESS_TOKEN")
    
    # Databricks configuration (from environment variables)
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
    DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "dvd_rental")
    
    # Primary keys for all tables
    PRIMARY_KEYS = {
        "actor": ["actor_id"],
        "address": ["address_id"],
        "category": ["category_id"],
        "city": ["city_id"],
        "country": ["country_id"],
        "customer": ["customer_id"],
        "film": ["film_id"],
        "film_actor": ["actor_id", "film_id"],
        "film_category": ["film_id", "category_id"],
        "inventory": ["inventory_id"],
        "language": ["language_id"],
        "payment": ["payment_id"],
        "rental": ["rental_id"],
        "staff": ["staff_id"],
        "store": ["store_id"]
    }
    
    print("=" * 60)
    print("Airbyte Setup: PostgreSQL → Databricks")
    print("=" * 60)
    
    try:
        # Initialize client with Client Credentials or Access Token
        if CLIENT_ID and CLIENT_SECRET:
            client = AirbyteClient(
                AIRBYTE_URL,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET
            )
        elif ACCESS_TOKEN:
            client = AirbyteClient(
                AIRBYTE_URL,
                api_key=ACCESS_TOKEN
            )
        else:
            raise Exception("Please provide either CLIENT_ID/CLIENT_SECRET or ACCESS_TOKEN")
        
        # Authenticate
        client.authenticate()
        
        # Auto-discover workspace (uses first workspace if multiple exist)
        client.get_workspace()
        
        # Check for existing resources first
        print("\n" + "=" * 60)
        print("Checking existing resources...")
        print("=" * 60)
        
        # Check existing sources
        sources_response = client.session.get(
            f"{client.base_url}/v1/sources?workspaceId={client.workspace_id}"
        )
        sources = sources_response.json().get("data", []) if sources_response.status_code == 200 else []
        postgres_source = next((s for s in sources if "dvd_rental" in s.get("name", "").lower()), None)
        
        if postgres_source:
            source_id = postgres_source["sourceId"]
            print(f"✓ Found existing PostgreSQL source: {source_id}")
        else:
            source_id = client.create_postgres_source()
        
        # Check existing destinations  
        dests_response = client.session.get(
            f"{client.base_url}/v1/destinations?workspaceId={client.workspace_id}"
        )
        dests = dests_response.json().get("data", []) if dests_response.status_code == 200 else []
        databricks_dest = next((d for d in dests if d.get("destinationType") == "databricks"), None)
        
        if databricks_dest:
            destination_id = databricks_dest["destinationId"]
            print(f"✓ Found existing Databricks destination: {destination_id}")
        else:
            destination_id = client.create_databricks_destination(
                host=DATABRICKS_HOST,
                http_path=DATABRICKS_HTTP_PATH,
                token=DATABRICKS_TOKEN,
                catalog=DATABRICKS_CATALOG,
                schema=DATABRICKS_SCHEMA
            )
            
        # Check existing connections
        print("\n" + "=" * 60)
        print("Setting up connection...")
        print("=" * 60)
        
        conns_response = client.session.get(
            f"{client.base_url}/v1/connections?workspaceId={client.workspace_id}"
        )
        conns = conns_response.json().get("data", []) if conns_response.status_code == 200 else []
        existing_conn = next((c for c in conns if c.get("sourceId") == source_id and c.get("destinationId") == destination_id), None)
        
        if existing_conn:
            connection_id = existing_conn["connectionId"]
            print(f"✓ Found existing connection: {connection_id}")
        else:
            # First, discover schema from source
            print("⟳ Discovering schema from PostgreSQL source...")
            
            try:
                catalog = client.get_source_schema(source_id)
                
                # Create connection with configured streams
                print("⟳ Creating connection with streams...")
                connection_id = client.create_connection_with_streams(
                    source_id=source_id,
                    destination_id=destination_id,
                    catalog=catalog,
                    primary_keys=PRIMARY_KEYS
                )
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    print("⚠ Schema discovery via API not permitted.")
                    print("  Creating connection - streams will be discovered on first refresh.")
                    
                    # Create a basic connection without streams
                    # We'll configure streams via the web catalog endpoint
                    connection_config = {
                        "name": "dvd_rental → Databricks",
                        "sourceId": source_id,
                        "destinationId": destination_id,
                        "schedule": {
                            "scheduleType": "manual"
                        },
                        "status": "active",
                        "dataResidency": "auto"
                    }
                    
                    response = client.session.post(
                        f"{client.base_url}/v1/connections",
                        headers=client.get_headers(),
                        json=connection_config
                    )
                    response.raise_for_status()
                    result = response.json()
                    connection_id = result.get("connectionId") or result.get("id")
                    print(f"✓ Created connection: {connection_id}")
                else:
                    raise
        
        # Now try to configure streams by fetching the connection's catalog
        # This is different from discovering - it uses the connection's own schema
        print("\n⟳ Configuring streams for connection...")
        try:
            # Get the connection details which includes its catalog
            conn_response = client.session.get(
                f"{client.base_url}/v1/connections/{connection_id}",
                headers=client.get_headers()
            )
            conn_response.raise_for_status()
            connection_data = conn_response.json()
            
            # Check if syncCatalog exists and has streams
            sync_catalog = connection_data.get("syncCatalog", {})
            if not sync_catalog.get("streams"):
                # No streams configured, need to discover and configure
                print("  No streams configured. Triggering schema refresh...")
                
                # Refresh the schema for this connection (this triggers discovery)
                refresh_response = client.session.post(
                    f"{client.base_url}/v1/sources/{source_id}/discover",
                    headers=client.get_headers(),
                    json={"sourceId": source_id, "connectionId": connection_id}
                )
                
                if refresh_response.status_code == 200:
                    catalog_data = refresh_response.json()
                    catalog = catalog_data.get("catalog", {})
                    
                    if catalog.get("streams"):
                        print(f"  ✓ Discovered {len(catalog['streams'])} streams")
                        
                        # Now update connection with streams
                        client.update_connection_streams(
                            connection_id=connection_id,
                            catalog=catalog,
                            primary_keys=PRIMARY_KEYS
                        )
                    else:
                        print("  ⚠ No streams found in catalog")
                else:
                    print(f"  ⚠ Schema refresh returned {refresh_response.status_code}")
                    print("  Please configure streams manually in the Airbyte UI:")
                    print(f"  http://localhost:8000/workspaces/{client.workspace_id}/connections/{connection_id}")
            else:
                print(f"  ✓ Connection already has {len(sync_catalog['streams'])} streams configured")
                
        except Exception as e:
            print(f"  ⚠ Could not auto-configure streams: {e}")
            print("  Please configure streams manually in the Airbyte UI:")
            print(f"  http://localhost:8000/workspaces/{client.workspace_id}/connections/{connection_id}")
        
        print("=" * 60)
        print("✓ Setup complete!")
        print("=" * 60)
        print(f"Source ID: {source_id}")
        print(f"Destination ID: {destination_id}")
        print(f"Connection ID: {connection_id}")
        print("\nYou can now trigger syncs manually from the Airbyte UI")
        print(f"or by running: python trigger_sync.py {connection_id}")
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
            if e.response.status_code in (401, 403):
                print("\n" + "=" * 60)
                print("Authentication Failed - Troubleshooting:")
                print("=" * 60)
                print("1. If using Airbyte Cloud or OSS with API keys enabled:")
                print("   - Go to Airbyte UI -> Settings -> API Keys")
                print("   - Create a new API key")
                print("   - Set AIRBYTE_API_KEY in this script")
                print("\n2. If using Airbyte OSS without API keys:")
                print("   - Verify your email/password are correct")
                print("   - Check that Airbyte is configured to allow API access")
                print("   - Try accessing http://localhost:8000/api/v1/workspaces/list in browser")
                print("\n3. Alternative: Use Airbyte UI to create connections manually")
                print("=" * 60)
        raise
    except Exception as e:
        print(f"✗ Error: {e}")
        if "authenticate" in str(e).lower() or "auth" in str(e).lower():
            print("\n" + "=" * 60)
            print("Authentication Issue - Get API Key:")
            print("=" * 60)
            print("1. Open Airbyte UI: http://localhost:8000")
            print("2. Go to Settings -> API Keys")
            print("3. Create a new API key")
            print("4. Set AIRBYTE_API_KEY in setup_airbyte.py")
            print("=" * 60)
        raise


if __name__ == "__main__":
    main()

