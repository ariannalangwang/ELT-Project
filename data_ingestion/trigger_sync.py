"""
Trigger a manual sync for an existing Airbyte connection.
Usage: python trigger_sync.py [connection_name_or_id]
Auto-discovers workspace and connection IDs from Airbyte API.
"""

import sys
import os
import requests
from setup_airbyte import AirbyteClient


def find_connection(client: AirbyteClient, name_or_id: str = None) -> tuple[str, str]:
    """
    Find connection ID by name or ID.
    If name_or_id is None, returns the first active connection.
    Returns (connection_id, connection_name).
    """
    try:
        # List all connections in the workspace
        response = client.session.get(
            f"{client.base_url}/v1/connections",
            headers=client.get_headers(),
            params={"workspaceId": client.workspace_id}
        )
        response.raise_for_status()
        connections = response.json().get("data", [])
        
        if not connections:
            raise Exception("No connections found in workspace")
        
        # If specific name/ID provided, search for it
        if name_or_id:
            # Try exact ID match first
            for conn in connections:
                if conn.get("connectionId") == name_or_id:
                    return conn["connectionId"], conn.get("name", "Unknown")
            
            # Try name match (case-insensitive, partial match)
            for conn in connections:
                if name_or_id.lower() in conn.get("name", "").lower():
                    return conn["connectionId"], conn.get("name", "Unknown")
            
            raise Exception(f"No connection found matching: {name_or_id}")
        
        # No name/ID provided - use first active connection
        for conn in connections:
            if conn.get("status") == "active":
                return conn["connectionId"], conn.get("name", "Unknown")
        
        # If no active connection, use the first one
        first_conn = connections[0]
        return first_conn["connectionId"], first_conn.get("name", "Unknown")
        
    except requests.exceptions.HTTPError as e:
        raise Exception(f"Failed to list connections: {e}")


def main():
    """Trigger a sync for a connection."""
    
    # Configuration
    AIRBYTE_URL = os.getenv("AIRBYTE_URL", "http://localhost:8000/api")
    CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
    CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
    
    # Optional: specify connection by name or ID via command line
    connection_identifier = sys.argv[1] if len(sys.argv) >= 2 else None
    
    try:
        # Initialize client (workspace will be auto-discovered)
        client = AirbyteClient(
            AIRBYTE_URL,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        client.authenticate()
        
        # Auto-discover workspace if not set
        if not client.workspace_id:
            client.get_workspace()
        
        print(f"✓ Using workspace: {client.workspace_id}")
        
        # Auto-discover connection
        connection_id, connection_name = find_connection(client, connection_identifier)
        print(f"✓ Found connection: {connection_name} ({connection_id})")
        
        # Trigger sync
        print(f"\n🔄 Triggering sync...")
        job_id = client.trigger_sync(connection_id)
        print(f"✓ Sync started successfully!")
        print(f"Job ID: {job_id}")
        print(f"\nMonitor at: http://localhost:8000/workspaces/{client.workspace_id}/connections/{connection_id}")
        
    except requests.exceptions.HTTPError as e:
        # Check if it's a 409 conflict (job already running)
        is_409 = False
        if hasattr(e, 'response') and e.response is not None:
            is_409 = e.response.status_code == 409
        elif "409" in str(e):
            is_409 = True
        
        if is_409:
            print(f"\n⚠ A sync is already running for this connection")
            sys.exit(0)
        else:
            print(f"\n✗ Error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

