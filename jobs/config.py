configuration = {
    "AWS_ACCESS_KEY": "your_access_key",
    "AWS_SECRET_KEY": "your_secret_key"
}

# How to get Security Credentials from AWS IAM?
# 1. Navigate to your user
# 2. Choose Security Credentials
# 3. Click "Create access key"
# 4. Choose "Application running outside AWS" (since it will be running in your script)
# 5. Enter any Description tag like: "temporary key"
# 6. Here you will have "Access key" and "Secret access key" for you configuration file in your script