export const config = {
  maxDuration: 30,
};

// In-memory cache (persists across warm invocations on same Vercel instance)
const cache = new Map();

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const { adv, cr } = req.query;
  if (!adv || !cr) {
    return res.status(400).json({ error: 'Missing adv or cr parameter' });
  }

  // Check cache
  const cacheKey = `${adv}:${cr}`;
  if (cache.has(cacheKey)) {
    return res.status(200).json(cache.get(cacheKey));
  }

  try {
    const reqData = new URLSearchParams({
      'f.req': JSON.stringify({ "1": adv, "2": cr, "5": { "1": 1 } })
    });

    const response = await fetch(
      'https://adstransparency.google.com/anji/_/rpc/LookupService/GetCreativeById?authuser=0',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
          'Origin': 'https://adstransparency.google.com',
          'Referer': 'https://adstransparency.google.com/',
        },
        body: reqData.toString(),
      }
    );

    if (response.status === 429) {
      return res.status(429).json({ error: 'rate_limited' });
    }

    const data = await response.json();
    const resp = (data && data['1']) || {};
    const creatives = resp['5'] || [];
    const fullText = JSON.stringify(creatives);

    let imageUrl = '';
    let embedUrl = '';

    // Strategy 1: Extract from HTML snippets in creative variants
    for (const variant of creatives) {
      const rawHtml = variant?.['3']?.['2'] || '';
      if (rawHtml) {
        // simgad URLs
        const simgad = rawHtml.match(/https?:\/\/tpc\.googlesyndication\.com[^\s'"\\<>]*simgad\/\d+/);
        if (simgad) { imageUrl = simgad[0]; break; }

        // 2mdn.net direct images
        const mdn = rawHtml.match(/https?:\/\/s\d+\.2mdn\.net\/[^\s'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)/i);
        if (mdn) { imageUrl = mdn[0]; break; }

        // img src from HTML
        const imgSrc = [...rawHtml.matchAll(/<img[^>]+src=["']([^"']+)["']/gi)];
        for (const m of imgSrc) {
          if (/2mdn\.net|googlesyndication\.com|googleusercontent\.com|gstatic\.com/.test(m[1])) {
            imageUrl = m[1]; break;
          }
        }
        if (imageUrl) break;

        // Sadbundle (HTML5)
        const sadbundle = rawHtml.match(/https?:\/\/tpc\.googlesyndication\.com\/archive\/sadbundle\/[^\s'"\\<>]+/);
        if (sadbundle) embedUrl = sadbundle[0];
      }
    }

    // Strategy 2: Resolve displayads URL
    if (!imageUrl) {
      for (const variant of creatives) {
        const displayUrl = variant?.['1']?.['4'] || variant?.['2']?.['4'] || '';
        if (displayUrl && displayUrl.includes('displayads')) {
          try {
            const dresp = await fetch(displayUrl, {
              headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' },
              redirect: 'follow',
            });
            const dtext = await dresp.text();

            const simgad2 = dtext.match(/https?:\/\/tpc\.googlesyndication\.com[^\s'"\\<>]*simgad\/\d+/);
            if (simgad2) { imageUrl = simgad2[0]; break; }

            const mdn2 = dtext.match(/https?:\/\/s\d+\.2mdn\.net\/[^\s'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)/i);
            if (mdn2) { imageUrl = mdn2[0]; break; }

            const yt = dtext.match(/https?:\/\/i\d*\.ytimg\.com\/vi\/([^/]+)\/[a-z]+\.jpg/);
            if (yt) { imageUrl = `https://i.ytimg.com/vi/${yt[1]}/hqdefault.jpg`; break; }

            const lh3 = dtext.match(/(?:https?:)?\/\/lh3\.googleusercontent\.com\/[^\s'"\\<>)]+/);
            if (lh3) {
              imageUrl = lh3[0].startsWith('//') ? 'https:' + lh3[0] : lh3[0];
              break;
            }
          } catch (e) { /* ignore */ }

          if (!embedUrl) embedUrl = displayUrl;
        }
      }
    }

    // Strategy 3: Deep search full response for any image URL
    if (!imageUrl) {
      const simgadAll = fullText.match(/https?:\/\/tpc\.googlesyndication\.com[^\s'"\\<>]*simgad\/\d+/);
      if (simgadAll) imageUrl = simgadAll[0];
    }
    if (!imageUrl) {
      const mdnAll = fullText.match(/https?:\/\/s\d+\.2mdn\.net\/[^\s'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)/i);
      if (mdnAll) imageUrl = mdnAll[0];
    }

    // Strategy 4: For displayads URLs, fetch the HTML version to extract actual image
    if (!imageUrl && embedUrl && embedUrl.includes('displayads')) {
      try {
        // Convert content.js to content.html and remove callback params
        let htmlUrl = embedUrl
          .replace('content.js', 'content.html')
          .replace(/&responseCallback=[^&]*/g, '')
          .replace(/&htmlParentId=[^&]*/g, '');
        const dresp2 = await fetch(htmlUrl, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' },
          redirect: 'follow',
        });
        if (dresp2.ok) {
          const dtext2 = await dresp2.text();
          // Extract image URLs from the rendered HTML
          const imgPatterns = [
            /https?:\/\/s\d+\.2mdn\.net\/[^\s'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)/i,
            /https?:\/\/tpc\.googlesyndication\.com[^\s'"\\<>]*simgad\/\d+/,
            /https?:\/\/[^\s'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)/i,
          ];
          for (const pat of imgPatterns) {
            const m = dtext2.match(pat);
            if (m && !m[0].includes('google.com/images/errors') && !m[0].includes('gstatic.com/images')) {
              imageUrl = m[0];
              break;
            }
          }
          // If still no direct image, use the HTML URL as embeddable iframe source
          if (!imageUrl) {
            embedUrl = htmlUrl;
          }
        }
      } catch (e) { /* ignore */ }
    }

    // Strategy 5: Search ALL nested objects for any image-like URL
    if (!imageUrl) {
      const allUrls = fullText.match(/https?:\/\/[^\s'"\\,\}\]]+\.(png|jpg|jpeg|gif|webp)/gi);
      if (allUrls) {
        for (const u of allUrls) {
          if (!u.includes('google.com/images/errors') && !u.includes('gstatic.com/images')) {
            imageUrl = u;
            break;
          }
        }
      }
    }

    // Debug mode: if ?debug=1, return raw creative data
    if (req.query.debug === '1') {
      return res.status(200).json({ imageUrl, embedUrl, _debug: { variantCount: creatives.length, fullTextLen: fullText.length, sample: fullText.substring(0, 2000) } });
    }

    // If we still only have a displayads JS URL, convert to HTML for iframe embedding
    if (!imageUrl && embedUrl && embedUrl.includes('content.js')) {
      embedUrl = embedUrl
        .replace('content.js', 'content.html')
        .replace(/&responseCallback=[^&]*/g, '')
        .replace(/&htmlParentId=[^&]*/g, '');
    }

    const result = { imageUrl, embedUrl: imageUrl ? '' : embedUrl };

    // Cache successful results
    if (imageUrl || embedUrl) {
      cache.set(cacheKey, result);
    }

    return res.status(200).json(result);
  } catch (err) {
    console.error('Preview fetch error:', err.message);
    return res.status(500).json({ error: 'Failed to fetch preview' });
  }
}
