-- GMRQB Query Template 5

SELECT * FROM variations
WHERE chromosome = ?
  AND location BETWEEN ? AND ?
  AND gender = ?
  AND population = '?'
  AND relationship = '?';
