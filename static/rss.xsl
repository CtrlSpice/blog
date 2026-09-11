<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="html" encoding="UTF-8"/>

  <xsl:template match="/rss/channel">
    <html lang="en">
      <head>
        <meta charset="UTF-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title><xsl:value-of select="title"/> RSS feed</title>
        <style>
          @font-face {
            font-family: 'Atkinson Hyperlegible Next';
            font-style: normal;
            font-weight: 400;
            font-display: swap;
            src: url('/fonts/atkinson-hyperlegible-next-latin-400-normal.woff2') format('woff2');
          }

          :root {
            color-scheme: light dark;
            --background: #faf4ed;
            --text: #575279;
            --muted: #797593;
            --accent: #286983;
          }

          * { box-sizing: border-box; }

          body {
            max-width: 720px;
            margin: 0 auto;
            padding: 3rem 1.5rem;
            color: var(--text);
            background: var(--background);
            font: 18px/1.6 'Atkinson Hyperlegible Next', system-ui, sans-serif;
          }

          header { margin-bottom: 3rem; }
          h1 { margin: 0; font-size: 2rem; line-height: 1.2; }
          header p { margin: 0.75rem 0 0; color: var(--muted); }
          a { color: var(--accent); text-underline-offset: 0.2em; }

          article { margin: 0 0 2rem; }
          article time { color: var(--muted); font-size: 0.8rem; }
          article h2 { margin: 0.15rem 0; font-size: 1.2rem; font-weight: 400; line-height: 1.3; }
          article p { margin: 0.35rem 0 0; color: var(--muted); font-size: 0.9rem; }

          @media (prefers-color-scheme: dark) {
            :root {
              --background: #191724;
              --text: #e0def4;
              --muted: #908caa;
              --accent: #9ccfd8;
            }
          }
        </style>
      </head>
      <body>
        <header>
          <h1><xsl:value-of select="title"/></h1>
          <p>This is an RSS feed. Subscribe by adding <a href="{atom:link/@href}" xmlns:atom="http://www.w3.org/2005/Atom"><xsl:value-of select="atom:link/@href"/></a> to your feed reader.</p>
        </header>

        <main>
          <xsl:for-each select="item">
            <article>
              <time><xsl:value-of select="substring(pubDate, 6, 11)"/></time>
              <h2><a href="{link}"><xsl:value-of select="title"/></a></h2>
              <p><xsl:value-of select="description"/></p>
            </article>
          </xsl:for-each>
        </main>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
