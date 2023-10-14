-- GMRQB Query Template 8

SELECT * FROM variations
WHERE chromosome = ?
  AND location BETWEEN ? AND ?
  AND quality BETWEEN ? AND ?
  AND depth BETWEEN ? AND ?
  AND allele_freq BETWEEN ? AND ?
  AND ref_base = '?'
  AND alt_base = '?'
  AND ancestral_allele = '?'
  AND variation_id BETWEEN ? AND ?
  AND sample_id BETWEEN ? AND ?
  AND gender = ?
  AND family_id BETWEEN ? AND ?
  AND population = '?'
  AND relationship = '?'
  AND variant_type = '?'
  AND genotype = '?'
  AND genotype_quality BETWEEN ? AND ?
  AND read_depth BETWEEN ? AND ?
  AND haplotype_quality BETWEEN ? AND ?;
