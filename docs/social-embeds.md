# Quoting social posts and discussions

Use `bluesky`, `twitter`, `mastodon`, `github`, `linkedin` or `hackernews` for a static quotation card.
Keep the original wording in the shortcode body; Markdown, links and multiple paragraphs are supported.
Cards use the blog's seasonal colours, with each network's logo beside the author.

Each shortcode takes the same parameters:

- `author`: the author's display name.
- `profile`: the author's profile URL.
- `href`: the original post, issue or comment URL. For comments, include the direct comment link.
- `date`: the date to display.
- `datetime`: the corresponding machine-readable ISO 8601 date or timestamp.
- `handle`: optional account handle, including the instance for Mastodon accounts.
- `avatar`: optional local image path. Save avatars in the post's page bundle or under `static/`, and supply their site-relative URL.

Use `twitter.com` links for Twitter cards.
For Mastodon, use the author's own instance URLs.
For GitHub, quote the issue body or comment in the card; use the separate `issue` shortcode for compact issue-list rows.
Hacker News cards can omit the avatar and handle, leaving the username and source link.

The cards contain no third-party embed scripts or engagement counts.
Their text remains readable without JavaScript.
Shared markup lives in `layouts/partials/social-embed.html`, and styling in `assets/css/extended/social-embeds.css`.
