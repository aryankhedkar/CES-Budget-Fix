# Setup Instructions

## To Push to GitHub

1. **Create a new repository on GitHub:**
   - Go to https://github.com/new
   - Repository name: `ces-budgets-fix` (or any name you prefer)
   - Make it **Private** (recommended, since it contains database connection info)
   - Don't initialize with README, .gitignore, or license

2. **Push the code:**
   ```bash
   cd ~/ces-budgets-fix
   git remote add origin https://github.com/YOUR_USERNAME/ces-budgets-fix.git
   git branch -M main
   git push -u origin main
   ```

   Replace `YOUR_USERNAME` with your GitHub username.

## Alternative: Using SSH

If you have SSH keys set up:
```bash
cd ~/ces-budgets-fix
git remote add origin git@github.com:YOUR_USERNAME/ces-budgets-fix.git
git branch -M main
git push -u origin main
```

## Share the Repository

Once pushed, you can share the repository link:
- **Public repo**: `https://github.com/YOUR_USERNAME/ces-budgets-fix`
- **Private repo**: Share via GitHub's "Settings" → "Collaborators" or generate a temporary access link
