-- GMRQB Query Template 4

SELECT * FROM variations
WHERE chromosome = ?
  AND location BETWEEN ? AND ?
  AND gender = ?
  AND population = '?';
