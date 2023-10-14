-- GMRQB Query Template 7

SELECT * FROM variations
WHERE chromosome = ?
  AND location BETWEEN ? AND ?
  AND gender = ?
  AND population = '?'
  AND relationship = '?'
  AND family_id BETWEEN ? AND ?
  AND variation_id BETWEEN ? AND ?;
