### Hi there, I'm a developer! 👋

Here are some of my certifications:

<!--START_SECTION:badges-->
<!--END_SECTION:badges-->

... more content about you ...
name: Update Credly Badges

on:
  workflow_dispatch:
  schedule:
    # Runs every 24 hours
    - cron: "0 0 * * *"

jobs:
  update-readme:
    name: Update Readme with Credly badges
    runs-on: ubuntu-latest
    steps:
      - name: Badges - Readme
        uses: papa-m-sanogo/badge-readme@main
        with:
          # Find your user ID in your public Credly profile URL
          # e.g., [https://www.credly.com/users/papa-m-sanogo/badges#credly]
          CREDLY_USER: your-credly-user-id
