# GitHub Repository Setup Guide

## Step 1: Create Repository on GitHub

1. Go to https://github.com and log in
2. Click the "+" icon in the top right corner
3. Select "New repository"
4. Fill in the details:
   - **Repository name**: `ring-project`
   - **Description**: "Cassandra Ring Analyzer - Visualize token ring distribution with gap detection"
   - **Visibility**: Choose Public or Private
   - **DO NOT** initialize with README, .gitignore, or license (we already have these)
5. Click "Create repository"

## Step 2: Initialize Git and Push (On Linux)

Open a terminal in your ring-project directory and run these commands:

```bash
# Navigate to the ring-project directory
cd /path/to/ring-project

# Initialize git repository
git init

# Add all files
git add .

# Create initial commit
git commit -m "Initial commit: Cassandra Ring Analyzer implementation

- Complete RingParser for parsing nodetool ring output
- TokenAnalyzer for range calculation and gap detection
- RingVisualizer for circular ring visualization
- Full CLI with multiple output formats
- Comprehensive documentation and specifications"

# Add your GitHub repository as remote (replace YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/ring-project.git

# Push to GitHub
git branch -M main
git push -u origin main
```

## Step 3: Verify Upload

1. Go to https://github.com/YOUR_USERNAME/ring-project
2. Verify all files are present:
   - README.md
   - cassandra_ring_analyzer.py
   - requirements.txt
   - .gitignore
   - docs/cassandra_ring_analyzer_spec.md
   - GITHUB_SETUP.md (this file)

## Alternative: Using SSH (Recommended for frequent use)

If you have SSH keys set up with GitHub:

```bash
# Use SSH URL instead
git remote add origin git@github.com:YOUR_USERNAME/ring-project.git
git branch -M main
git push -u origin main
```

## Future Updates

After making changes to your code:

```bash
# Check what changed
git status

# Add changed files
git add .

# Commit with a descriptive message
git commit -m "Description of your changes"

# Push to GitHub
git push
```

## Common Git Commands

```bash
# View commit history
git log --oneline

# View current status
git status

# View differences
git diff

# Create a new branch
git checkout -b feature-name

# Switch branches
git checkout main

# Merge a branch
git merge feature-name
```

## Troubleshooting

### If you get "remote origin already exists"
```bash
git remote remove origin
git remote add origin https://github.com/YOUR_USERNAME/ring-project.git
```

### If you need to force push (use carefully!)
```bash
git push -f origin main
```

### If you want to include the sample ring file
Edit `.gitignore` and remove or comment out the line:
```
# ring
```

Then:
```bash
git add ring
git commit -m "Add sample ring file"
git push