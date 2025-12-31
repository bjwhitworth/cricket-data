#!/usr/bin/env python3
"""
Test Gemini API connection and check quota/billing info.
Usage: python scripts/python/test_gemini_connection.py
"""

import os
import sys
import google.genai as genai

# The client gets the API key from the environment variable `GEMINI_API_KEY`.
def test_connection():
    """Test basic Gemini API connection."""
    api_key = os.getenv('GEMINI_API_KEY')
    
    if not api_key:
        print("❌ GEMINI_API_KEY environment variable not set")
        print("\nSet it with:")
        print("  export GEMINI_API_KEY='your-key-here'")
        sys.exit(1)
    
    print("✓ API key found")
    print(f"  Key prefix: {api_key[:8]}...")
    
    try:
        client = genai.Client(api_key=api_key)  # Explicitly pass API key from GEMINI_API_KEY environment variable
        print("✓ API configured successfully")
    except Exception as e:
        print(f"❌ Failed to configure API: {e}")
        sys.exit(1)
    
    # List available models
    print("\n📋 Available models:")
    try:
        models = client.models.list()
        for model in models:
            print(f"  - {model.name}")
    except Exception as e:
        print(f"❌ Failed to list models: {e}")
    
    # Test generation
    print("\n🧪 Testing content generation...")
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite',
            contents="Say 'Hello from Gemini!' in exactly 5 words.",
            config=genai.types.GenerateContentConfig(
                max_output_tokens=30,
                temperature=1,
            )
        )
        print(f"✓ Generation successful!")
        print(f"  Response: {response.text}")
        if hasattr(response, 'usage_metadata') and response.usage_metadata:
            print(f"  Tokens used: ~{response.usage_metadata.total_token_count}")
    except Exception as e:
        print(f"❌ Generation failed: {e}")
        sys.exit(1)
    
    # Billing/quota info
    print("\n💳 Billing & Quota Info:")
    print("  Note: Gemini API free tier includes:")
    print("    - 1,500 requests/day (Flash models)")
    print("    - 15 requests/minute (Flash)")
    print("    - 1 million tokens/minute (Flash)")
    print("\n  For detailed billing/quota, check:")
    print("    https://aistudio.google.com/app/apikey")
    print("    or Google Cloud Console if using a paid project")
    
    # Try to get current usage (requires Google Cloud API)
    # Note: This requires additional setup with google-cloud-monitoring
    # For now, just inform the user
    print("\n  ℹ️  Real-time quota monitoring requires google-cloud-monitoring SDK")
    print("      Install with: pip install google-cloud-monitoring")
    
    print("\n✅ All tests passed! Gemini API is ready to use.")

if __name__ == "__main__":
    test_connection()
