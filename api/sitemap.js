const { createClient } = require('@supabase/supabase-js');

module.exports = async (req, res) => {
  console.log('Sitemap function started');

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseAnonKey) {
    console.error('Missing env vars');
    return res.status(500).send('Missing environment variables');
  }

  const supabase = createClient(supabaseUrl, supabaseAnonKey);

  try {
    const { data: jobs, error } = await supabase
      .from('ApplyMore')
      .select('id, created_at')
      .order('created_at', { ascending: false });

    if (error) throw error;

    const baseUrl = 'https://applymore.vercel.app';
    const today = new Date().toISOString().split('T')[0];

    let xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`;
    xml += `<url><loc>${baseUrl}/</loc><lastmod>${today}</lastmod><changefreq>daily</changefreq><priority>1.0</priority></url>`;
    for (const job of jobs || []) {
      const lastmod = job.created_at ? job.created_at.split('T')[0] : today;
      xml += `<url><loc>${baseUrl}/job.html?id=${job.id}</loc><lastmod>${lastmod}</lastmod><changefreq>weekly</changefreq><priority>0.8</priority></url>`;
    }
    xml += `</urlset>`;

    res.setHeader('Content-Type', 'application/xml');
    res.status(200).send(xml);
  } catch (err) {
    console.error('Sitemap error:', err);
    res.status(500).send('Error: ' + err.message);
  }
};
