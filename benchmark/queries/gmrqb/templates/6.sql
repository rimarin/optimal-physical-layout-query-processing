-- GMRQB Query Template 6

SELECT * FROM variations
WHERE chromosome = ?
  AND location BETWEEN ? AND ?
  AND gender = ?
  AND population = '?'
  AND relationship = '?'
  AND family_id BETWEEN ? AND ?;
