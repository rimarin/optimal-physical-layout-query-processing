-- GMRQB Query Template 3

SELECT * FROM variations
WHERE chromosome = ?
  AND location BETWEEN ? AND ?
  AND gender = ?;
