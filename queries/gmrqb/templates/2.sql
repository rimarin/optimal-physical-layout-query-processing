-- GMRQB Query Template 2

SELECT * FROM variations
WHERE chromosome = ?
  AND location BETWEEN ? AND ?
  AND quality BETWEEN ? AND ?
  AND depth BETWEEN ? AND ?
  AND allele_freq BETWEEN ? AND ?;
