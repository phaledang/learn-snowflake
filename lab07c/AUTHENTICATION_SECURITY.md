# Azure AD Authentication Security Configuration

## 🔒 Security Best Practices

This application uses **secure random ports** for OAuth redirect URIs to prevent port hijacking attacks.

### Why Random Ports Are More Secure:

1. **Prevents Port Hijacking**: Fixed ports allow malicious apps to steal authorization codes
2. **MSAL Best Practice**: Microsoft's recommended approach for desktop applications
3. **Dynamic Port Assignment**: OS assigns available ports, reducing conflicts

### Azure AD App Registration Configuration:

To support secure random ports, configure your Azure AD app registration:

1. **Go to**: Azure Portal → Azure AD → App registrations → Your App
2. **Navigate to**: Authentication section
3. **Add Platform**: Mobile and desktop applications
4. **Add Redirect URI**: `http://localhost` (without port number)
5. **Save** the configuration

This configuration accepts any localhost port, supporting MSAL's secure random port generation.

### Alternative Authentication Methods:

- **Device Code Flow**: No redirect URI needed, works on any device
- **Integrated Windows Authentication**: Uses Azure CLI or system credentials
- **Interactive Browser (Random Port)**: Secure MSAL-managed authentication

### Security Benefits:

✅ **Port Hijacking Protection**: Random ports prevent malicious interception
✅ **MSAL Security Model**: Follows Microsoft's security recommendations  
✅ **Cross-Platform Compatibility**: Works on all platforms with localhost support
✅ **No Fixed Dependencies**: No need to reserve specific ports

## Configuration Required:

### Azure AD App Registration:
- **Redirect URI**: `http://localhost` (accepts any port)
- **Platform**: Mobile and desktop applications
- **Public Client**: Yes (no client secret required)

### Environment Variables:
```
AZURE_AD_CLIENT_ID=your_client_id_here
AZURE_AD_TENANT_ID=your_tenant_id_here
```

No redirect URI environment variable needed - MSAL handles this securely.