module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.status(200).send('<h1>Function works</h1>');
};
