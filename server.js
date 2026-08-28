const express = require('express');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', engine: 'Promotion Engine 3000', recommendation: 'IMMEDIATE_PROMOTION' });
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
