export const config = {
  maxDuration: 300,
};

export default async function handler(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { competitorName, platform, ads, imageUrls } = req.body;

  if (!competitorName || !ads) {
    return res.status(400).json({ error: 'Missing competitorName or ads' });
  }

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: 'OPENAI_API_KEY not configured' });
  }

  // Build the analysis prompt
  const systemPrompt = `Task:
Analyze competitor Meta Ads and TikTok Ads in bulk to extract the most useful paid social creative insights we can reuse in our own business.

Goal:
Identify what is likely helping performance, what is likely hurting performance, what patterns repeat across competitors, and what specific ideas are worth testing.

Inputs:
You may receive both image ads and video ads.
You may analyze many ads at once.
Some ads may be low-quality, repetitive, incomplete, weak, or hard to assess.

Operating rules:
1. Write like a tough senior paid social creative strategist, not a polite reviewer and not a design critic.
2. Be concise, structured, commercially sharp, and non-redundant.
3. Do not analyze every ad with equal depth.
   - Compress low-signal or ordinary ads.
   - Expand only standout ads, highly reusable ads, or ads with obvious strategic mistakes.
4. Separate facts from inference.
   - Facts = what is directly visible or explicitly stated in the ad.
   - Inference = your hypothesis about why it may work or fail.
5. Do not invent performance results, audience intent, or business outcomes.
6. If evidence is weak, incomplete, blurry, cropped, missing audio, or otherwise unreliable, say so clearly.
7. Judge all creative elements through paid social performance logic:
   - scroll-stop / thumbstop potential
   - attention retention
   - message clarity
   - trust and credibility
   - click intent
   - conversion intent
8. Evaluate creative variables as performance drivers, not just design choices:
   - hook, objects shown, product visibility
   - people / faces / expression / body language / eye direction
   - colors, contrast, framing, composition
   - text overlays, background / setting
   - motion / editing / pacing
   - offer presentation, CTA, proof points

Critical scoring discipline:
Be stricter than a normal reviewer.
Do not reward ads just for being clean, branded, or professionally produced.
Do not assume strong performance without strong creative evidence.

Use this mindset:
- Most ads are average.
- Many ads are strategically weak even if they look polished.
- A good-looking ad is not automatically an effective ad.
- A branded ad is not automatically a strong ad.
- A live ad is not proof that the creative itself is strong.
- Long-running ads may survive because of spend, audience quality, offer strength, or brand power, not necessarily because the creative is excellent.

Scoring calibration:
Use scores consistently and do not inflate them.
- 9-10 = exceptional, rare, clearly above category norm, strong evidence of creative excellence
- 7-8 = genuinely strong, likely performance-positive, with clear strengths
- 5-6 = average, functional, but not distinctive or not fully convincing
- 3-4 = weak, likely underperforming, unclear, generic, or strategically poor
- 1-2 = very poor, actively hurting performance or failing key basics

Anti-inflation rules:
- Start from a neutral assumption of 5, not 8.
- Do not give 8+ unless the ad clearly earns it.
- If the ad has weak clarity, weak offer communication, or weak conversion logic, cap scores accordingly.
- If the ad is visually distinctive but strategically vague, keep the score modest.
- If the ad is generic, overused, low-trust, or easily forgettable, use lower scores confidently.
- Do not avoid giving 3s or 4s when warranted.
- Only a small minority of ads should score 8 or above unless the batch is truly exceptional.

When in doubt, underrate rather than overrate. Strong scores must be earned by clear creative evidence, not assumed from polish, branding, or market presence.

Evaluation framework:
For each ad, assess only the most performance-relevant points under these categories:

A. Platform fit — Best fit: Meta, TikTok, or both. Whether the ad feels native to the platform.
B. Attention capture — For images: focal point, immediate relevance/curiosity/contrast. For videos: first 1-3 seconds, hook type. For both: attention mechanism used.
C. Message and offer — Core message, value prop, offer strength, clarity, proof points, CTA strength.
D. Visual execution — Objects, product prominence, human presence, composition, colors, branding, text readability, background, motion/pacing.
E. Consumer psychology — What desire/fear/frustration/aspiration is activated. Trust, urgency, curiosity, relatability, authority.
F. Performance hypothesis — What is most likely helping or hurting: scroll-stop rate, hold rate, CTR, CVR, trust, qualified vs low-quality clicks.
G. Reusability — What is reusable for our business, what is risky/weak, what is too brand-specific, what deserves testing.

Important analysis discipline:
For each ad, clearly distinguish: Observations, Performance hypotheses, Confidence level (high/medium/low).
Use "high" confidence only when evidence is directly visible and strong.
Use "low" confidence when the ad is ambiguous or key context is missing.

Formatting requirements:
Use clean Markdown.
Make the response feel like a sharp Notion-style analysis doc.
Use:
- clear H1 / H2 / H3 headers
- compact tables
- short bullets
- collapsible <details> sections for deeper dives on standout ads

Do not output long unbroken paragraphs.
Do not bury key conclusions in the middle of the response.

Output structure:

# TL;DR
At the very top, provide a short executive summary with:
- Overall verdict on the batch
- 3-5 key insights
- 3 biggest weaknesses across the ads
- 3 best opportunities to reuse or test
- 2-5 specific ads worth reviewing first
Keep TL;DR short, sharp, and highly skimmable.

# 1. Bulk summary

## Batch overview
- Total ads reviewed: X
- Image ads: X
- Video ads: X
- Overall quality of the batch: strong / mixed / weak

## Strategic summary
- Core acquisition themes:
- Repeated winning patterns:
- Repeated weak patterns:
- Most common hook patterns:
- Most common visual patterns:
- Most common messaging patterns:
- Biggest reusable opportunities:
- Biggest white-space opportunities:

## Top 20% most strategically interesting ads
List only the few ads that are unusually effective, highly reusable, strategically revealing, or unusually weak in a useful way.
For each: Ad label, Format, Why it matters in 1 sentence.

# 2. Image ads

## Image ads - comparison table
| Ad label | Preview link | Platform fit | Verdict | Caught my eye | Scroll-stop | Message clarity | Visual hierarchy | Brand / offer communication | Conversion potential | Confidence | Key finding |
|----------|--------------|--------------|---------|---------------|------------:|----------------:|-----------------:|----------------------------:|--------------------:|------------|-------------|

Rules:
- Sort rows by strategic importance: ads mentioned in TL;DR "Ads worth reviewing first" come first, then remaining ads sorted by overall score (highest to lowest).
- Preview link is mandatory. Use the direct image asset URL (the fbcdn or similar URL provided in the ad data).
- Key finding must be one short sentence only.
- Keep rows compact and highly scannable.
- Prefer table output over narrative.
- Do not write full per-ad essays for ordinary image ads.

## Standout image ads
Only include ads where "Caught my eye = Yes".
For each standout image ad, use this collapsible format:

<details>
<summary><strong>[Ad label]</strong> — why it stands out in 1 short sentence</summary>

**Preview link:**
[Insert link]

**Snapshot**
- Verdict:
- Platform fit:
- Confidence:

**Observations**
- 2-4 bullets

**Performance hypotheses**
- 2-4 bullets

**What we can reuse**
- 2-3 bullets

**What to avoid**
- 1-2 bullets

**Test ideas**
- 2 bullets

</details>

# 3. Video ads

## Video ads
For each video ad:

### [Ad label]

**Snapshot**
- Preview link:
- Platform fit:
- Verdict:
- Caught my eye:
- Confidence:

**Quick summary**
- 2-3 sentences max

**Observations**
- 3-5 bullets max

**Performance hypotheses**
- 3-5 bullets max

**Score table**
| Criterion | Score (1-10) | Comment |
|-----------|-------------:|---------|
| Hook strength in first 3 seconds | ... | ... |
| Attention retention / pacing | ... | ... |
| Message clarity | ... | ... |
| Brand / offer communication | ... | ... |
| Conversion potential | ... | ... |

**Key takeaways**
- Strengths: 3 bullets max
- Weaknesses: 3 bullets max
- Triggers used: short comma-separated list
- What we can reuse: 2-3 bullets
- What to avoid: 1-2 bullets
- Test ideas: 2-3 bullets

Compression rule: If a video is ordinary or low-signal, keep all sections brief. If standout, go slightly deeper.

# 4. Final synthesis

## Best learnings from image ads
## Best learnings from video ads
## Hooks worth testing
## Visual patterns worth testing
## Messaging angles worth testing
## Common mistakes to avoid
## Standout ads worth deeper review
- [Ad label] — why it matters
## White-space opportunities competitors are missing

Writing style: Direct, clear, insight-dense, critical when needed, no fluff, no generic praise, no repeated points. Optimize for readability and decision usefulness.
Use markdown formatting.`;

  // Build content array with text + images
  const content = [];

  // Text summary of the ads
  const textSummary = `Analyze the ad campaign for **${competitorName}** on **${platform}**.

Here's the data:
${ads}

Please analyze their creative strategy, identify top-performing ads (longest-running = best performing),
and provide actionable insights on what makes their ads effective or ineffective.`;

  content.push({ type: 'text', text: textSummary });

  // Download images and convert to base64 (fbcdn URLs are temporary/authenticated)
  if (imageUrls && imageUrls.length > 0) {
    content.push({ type: 'text', text: '\n\nHere are sample ad creatives to analyze visually:' });
    const downloadPromises = imageUrls.slice(0, 6).map(async (url) => {
      try {
        const imgResp = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
        if (!imgResp.ok) return null;
        const buffer = await imgResp.arrayBuffer();
        const base64 = Buffer.from(buffer).toString('base64');
        const contentType = imgResp.headers.get('content-type') || 'image/jpeg';
        return `data:${contentType};base64,${base64}`;
      } catch (e) {
        return null;
      }
    });
    const base64Images = (await Promise.all(downloadPromises)).filter(Boolean);
    for (const dataUrl of base64Images) {
      content.push({
        type: 'image_url',
        image_url: { url: dataUrl, detail: 'low' },
      });
    }
  }

  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: 'gpt-5.4-nano',
        messages: [
          { role: 'developer', content: systemPrompt },
          { role: 'user', content },
        ],
        max_completion_tokens: 16000,
      }),
    });

    if (!response.ok) {
      const err = await response.text();
      console.error('OpenAI error:', err);
      return res.status(502).json({ error: 'OpenAI API error: ' + err });
    }

    const data = await response.json();
    const analysis = data.choices[0].message.content;

    return res.status(200).json({ analysis });
  } catch (err) {
    console.error('Error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}
