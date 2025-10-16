# Azure AD App Registration Configuration for Desktop Authentication

## Problem Identified

The current Azure AD app registration (`ed4ce18a-2930-47d5-ab28-8c020ba4f41f`) is configured as a **confidential client** (web application), but we're trying to use it for **desktop authentication** which requires a **public client**.

## Error Analysis

```
AADSTS7000218: The request body must contain the following parameter: 'client_assertion' or 'client_secret'
```

This error occurs because:
- The app is registered as a "Web" application type
- Web applications require client secrets for security
- Desktop applications should NOT have client secrets (they can't be kept secure)

## Solution Options

### Option 1: Create New Public Client App Registration (Recommended)

1. Go to Azure Portal → Azure Active Directory → App registrations
2. Click "New registration"
3. Configure:
   - **Name**: "Lab07c Desktop Assistant" 
   - **Supported account types**: Choose appropriate option (usually "Accounts in this organizational directory only")
   - **Redirect URI**: Select "Public client/native (mobile & desktop)" and enter:
     - `http://localhost` (for interactive browser flow)
     - `urn:ietf:wg:oauth:2.0:oob` (for device code flow)

4. After creation, go to "Authentication" settings:
   - Under "Advanced settings" → "Allow public client flows" → Select **Yes**
   - Add additional redirect URIs if needed:
     - `http://localhost:8000/auth/callback`
     - `http://localhost:3000/auth/callback`

5. Go to "API permissions":
   - Add "Microsoft Graph" → "Delegated permissions" → "User.Read"
   - Grant admin consent if required

6. Copy the new **Application (client) ID** and update your `.env` file

### Option 2: Convert Existing App to Public Client

1. Go to Azure Portal → Azure Active Directory → App registrations
2. Find your app: `ed4ce18a-2930-47d5-ab28-8c020ba4f41f`
3. Go to "Authentication" settings:
   - Under "Advanced settings" → "Allow public client flows" → Select **Yes**
   - Add platform → "Mobile and desktop applications"
   - Add redirect URIs:
     - `http://localhost`
     - `urn:ietf:wg:oauth:2.0:oob`

4. Remove any client secrets from "Certificates & secrets" (not needed for public clients)

## Authentication Flow Types by App Type

| App Type | Interactive Browser | Device Code | Integrated Windows |
|----------|-------------------|-------------|-------------------|
| Public Client | ✅ | ✅ | ✅ |
| Confidential Client | ❌ (needs secret) | ❌ (needs secret) | ❌ (needs secret) |

## Recommended Configuration for Desktop Apps

```env
# For Public Client (Desktop App)
AZURE_AD_CLIENT_ID=your-new-public-client-id
AZURE_AD_TENANT_ID=a3e8f00c-1024-4869-a9e1-96ca13af6290
AZURE_AD_REDIRECT_URI=http://localhost
```

## Testing After Configuration

Run the following authentication tests:

```bash
# Test device code (works with any configuration)
python test_device_auth.py

# Test interactive browser (requires public client)
python test_api_auth.py
```

## Security Notes

- **Public clients** cannot securely store secrets (appropriate for desktop/mobile)
- **Confidential clients** can securely store secrets (appropriate for web servers)
- Desktop applications should ALWAYS be public clients
- Use device code flow as fallback for public clients

## Current Status

- ❌ Current app (`ed4ce18a-2930-47d5-ab28-8c020ba4f41f`) is confidential client
- ❌ Interactive browser authentication failing
- ❌ Device code authentication failing  
- ✅ Need to reconfigure as public client or create new public client app

## Next Steps

1. Choose Option 1 or Option 2 above
2. Update `.env` with new configuration
3. Test authentication flows
4. Implement fallback flows (device code → integrated → manual token entry)