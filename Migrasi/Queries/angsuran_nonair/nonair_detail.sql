DROP TABLE IF EXISTS __tmp_nonair;
CREATE TABLE __tmp_nonair AS
SELECT 
a.id AS idangsuran,
a.jumlahtermin,
a.noangsuran AS noangsuran1,
b.*
FROM `daftarangsuran` a
JOIN nonair b ON b.`urutan`=a.`urutan_nonair`
WHERE a.`keperluan`<>'JNS-36' AND b.jenis<>'JNS-38';

SELECT
@idpdam,
p.id AS idnonair,
'Biaya' AS parameter,
p.postbiaya AS postbiaya,
p.value AS `value`,
NOW() AS waktuupdate
FROM (
SELECT id,'biayapemakaian' AS parameter,'biayapemakaian' AS postbiaya,CASE WHEN biayapemakaian>0 THEN biayapemakaian ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'administrasi' AS parameter,'administrasi' AS postbiaya,CASE WHEN administrasi>0 THEN administrasi ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'pemeliharaan' AS parameter,'pemeliharaan' AS postbiaya,CASE WHEN pemeliharaan>0 THEN pemeliharaan ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'retribusi' AS parameter,'retribusi' AS postbiaya,CASE WHEN retribusi>0 THEN retribusi ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'dendatunggakan' AS parameter,'dendatunggakan' AS postbiaya,CASE WHEN dendatunggakan>0 THEN dendatunggakan ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'ppn' AS parameter,'ppn' AS postbiaya,CASE WHEN ppn>0 THEN ppn ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'biayabahan' AS parameter,'biayabahan' AS postbiaya,CASE WHEN biayabahan>0 THEN biayabahan ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'biayapasang' AS parameter,'biayapasang' AS postbiaya,CASE WHEN biayapasang>0 THEN biayapasang ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'pendaftaran' AS parameter,'pendaftaran' AS postbiaya,CASE WHEN pendaftaran>0 THEN pendaftaran ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'jaminan' AS parameter,'jaminan' AS postbiaya,CASE WHEN jaminan>0 THEN jaminan ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'perencanaan' AS parameter,'perencanaan' AS postbiaya,CASE WHEN perencanaan>0 THEN perencanaan ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'meterai' AS parameter,'meterai' AS postbiaya,CASE WHEN meterai>0 THEN meterai ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'tangki' AS parameter,'tangki' AS postbiaya,CASE WHEN tangki>0 THEN tangki ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'perbaikan' AS parameter,'perbaikan' AS postbiaya,CASE WHEN perbaikan>0 THEN perbaikan ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'biayagantinama' AS parameter,'biayagantinama' AS postbiaya,CASE WHEN biayagantinama>0 THEN biayagantinama ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'biayaprosestutup' AS parameter,'biayaprosestutup' AS postbiaya,CASE WHEN biayaprosestutup>0 THEN biayaprosestutup ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'biayaprosesbuka' AS parameter,'biayaprosesbuka' AS postbiaya,CASE WHEN biayaprosesbuka>0 THEN biayaprosesbuka ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'dendapelanggaran' AS parameter,'dendapelanggaran' AS postbiaya,CASE WHEN dendapelanggaran>0 THEN dendapelanggaran ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'jasadaribahan' AS parameter,'jasadaribahan' AS postbiaya,CASE WHEN jasadaribahan>0 THEN jasadaribahan ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'penyesuaian' AS parameter,'penyesuaian' AS postbiaya,CASE WHEN penyesuaian>0 THEN penyesuaian ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'lainnya' AS parameter,'lainnya' AS postbiaya,CASE WHEN lainnya>0 THEN lainnya ELSE NULL END AS `value` FROM __tmp_nonair
UNION ALL SELECT id,'realisasisambungan' AS parameter,'lainnya' AS postbiaya,CASE WHEN realisasisambungan>0 THEN realisasisambungan ELSE NULL END AS `value` FROM __tmp_nonair
) p
WHERE p.value IS NOT NULL;