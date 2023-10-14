-- GMRQB Query Template 1

SELECT * FROM variations
WHERE chromosome = ?
  AND location BETWEEN ? AND ?;

