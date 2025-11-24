"""
Trigger a manual sync for an existing Airbyte connection.
Usage: python trigger_sync.py [connection_id]
If connection_id is not provided, uses the default from configuration.
"""

import sys
import os
import requests
from setup_airbyte import AirbyteClient


def main():
    """Trigger a sync for a connection."""
    
    # ========================================
    # CONFIGURATION - Update these values
    # ========================================
    # Airbyte instance configuration (non-sensitive)
    AIRBYTE_URL = "http://localhost:8000/api"  # Your Airbyte API URL
    WORKSPACE_ID = "your-workspace-id-here"     # Your Airbyte workspace ID
    CONNECTION_ID = "your-connection-id-here"   # Your Airbyte connection ID
    
    # Credentials (from environment variables - keep as secrets)
    CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
    CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
    
    # Allow connection_id override from command line
    if len(sys.argv) >= 2:
        connection_id = sys.argv[1]
    else:
        connection_id = CONNECTION_ID
        print(f"Using default connection ID from configuration: {connection_id}")
    
    try:
        client = AirbyteClient(
            AIRBYTE_URL,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            workspace_id=WORKSPACE_ID
        )
        client.authenticate()
        
        print(f"Triggering sync for connection: {connection_id}")
        job_id = client.trigger_sync(connection_id)
        print(f"\n✓ Sync started successfully!")
        print(f"Job ID: {job_id}")
        print(f"\nMonitor the sync at: http://localhost:8000/workspaces/{client.workspace_id}/connections/{connection_id}")
        
    except requests.exceptions.HTTPError as e:
        # Check if it's a 409 conflict (job already running)
        is_409 = False
        if hasattr(e, 'response') and e.response is not None:
            is_409 = e.response.status_code == 409
        elif "409" in str(e):
            is_409 = True
        
        if is_409:
            print(f"\n⚠ A sync is already running for this connection")
            print(f"\nMonitor at: http://localhost:8000/workspaces/{WORKSPACE_ID}/connections/{connection_id}")
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

