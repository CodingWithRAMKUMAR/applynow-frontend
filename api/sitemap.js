const { createClient } = require('@supabase/supabase-js');

module.exports = async (req, res) => {
  console.log('Sitemap function started');

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

  // Check environment variables
  if (!supabaseUrl) {
    console.error('SUPABASE_URL is missing');
    return res.status(500).send('Missing SUPABASE_URL');
  }
  if (!supabaseAnonKey) {
    console.error('SUPABASE_ANON_KEY is missing');
    return res.status(500).send('Missing SUPABASE_ANON_KEY');
  }

  console.log('Both env vars are present');

  try {
    const supabase = createClient(supabaseUrl, supabaseAnonKey);
    
    console.log('Attempting to fetch jobs from Supabase...');
    const { data: jobs, error } = await supabase
      .from('ApplyMore')
      .select('id, created_at')
      .eq('is_hidden', false)
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Supabase query error:', error);
      return res.status(500).send('Supabase error: ' + error.message);
    }

    console.log(`Found ${jobs ? jobs.length : 0} jobs`);

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
    console.error('Unexpected error:', err);
    res.status(500).send('Internal Server Error: ' + err.message);
  }
};
