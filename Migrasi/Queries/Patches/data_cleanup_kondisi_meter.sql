UPDATE pelanggan SET kodekondisimeter='1' WHERE kodekondisimeter IN ('','-');

REPLACE INTO kondisimeter (kodekondisi, kondisi)
SELECT
kodekondisimeter,
kodekondisimeter
FROM pelanggan
WHERE kodekondisimeter NOT IN (SELECT kodekondisimeter FROM kondisimeter)
GROUP BY kodekondisimeter;