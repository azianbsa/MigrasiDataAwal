DROP TEMPORARY TABLE IF EXISTS __tmp_nonair_hapus;
CREATE TEMPORARY TABLE __tmp_nonair_hapus AS
SELECT id FROM [table] WHERE flaghapus=1;

SELECT a.* FROM (
SELECT
@idpdam,
SUBSTRING_INDEX(GROUP_CONCAT(a.`id` ORDER BY id),',',1) AS id,
'Meterai' AS parameter,
'meterai' AS postbiaya,
b.total AS `value`,
NOW() AS waktuupdate
FROM [table] a
JOIN (
SELECT nomor,total FROM [table] WHERE flaghapus=0 AND jenis='JNS-16' GROUP BY nomor
) b ON b.nomor=a.nomor
GROUP BY a.`nomor`
) a
LEFT JOIN __tmp_nonair_hapus b ON b.id=a.id
WHERE b.id IS NULL