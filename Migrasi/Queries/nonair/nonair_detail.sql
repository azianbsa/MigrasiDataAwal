DROP TABLE IF EXISTS __tmp_nonair;
CREATE TABLE __tmp_nonair AS
SELECT
@id:=@id+1 AS id,
urutan
FROM [table]
,(SELECT @id:=@lastid) AS id
WHERE flaghapus=0 AND flagangsur=0 AND jenis NOT IN ('JNS-16','JNS-38');

SELECT
@idpdam,
p.id AS idnonair,
p.parameter AS parameter,
p.postbiaya AS postbiaya,
p.value AS `value`,
p.waktuupdate AS waktuupdate
FROM (
SELECT b.id,'biayapemakaian' AS parameter,'biayapemakaian' AS postbiaya,CASE WHEN biayapemakaian>0 THEN biayapemakaian ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'administrasi' AS parameter,'administrasi' AS postbiaya,CASE WHEN administrasi>0 THEN administrasi ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'pemeliharaan' AS parameter,'pemeliharaan' AS postbiaya,CASE WHEN pemeliharaan>0 THEN pemeliharaan ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'retribusi' AS parameter,'retribusi' AS postbiaya,CASE WHEN retribusi>0 THEN retribusi ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'dendatunggakan' AS parameter,'dendatunggakan' AS postbiaya,CASE WHEN dendatunggakan>0 THEN dendatunggakan ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'ppn' AS parameter,'ppn' AS postbiaya,CASE WHEN ppn>0 THEN ppn ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'biayabahan' AS parameter,'biayabahan' AS postbiaya,CASE WHEN biayabahan>0 THEN biayabahan ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'biayapasang' AS parameter,'biayapasang' AS postbiaya,CASE WHEN biayapasang>0 THEN biayapasang ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'pendaftaran' AS parameter,'pendaftaran' AS postbiaya,CASE WHEN pendaftaran>0 THEN pendaftaran ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'jaminan' AS parameter,'jaminan' AS postbiaya,CASE WHEN jaminan>0 THEN jaminan ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'perencanaan' AS parameter,'perencanaan' AS postbiaya,CASE WHEN perencanaan>0 THEN perencanaan ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'meterai' AS parameter,'meterai' AS postbiaya,CASE WHEN meterai>0 THEN meterai ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'tangki' AS parameter,'tangki' AS postbiaya,CASE WHEN tangki>0 THEN tangki ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'perbaikan' AS parameter,'perbaikan' AS postbiaya,CASE WHEN perbaikan>0 THEN perbaikan ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'biayagantinama' AS parameter,'biayagantinama' AS postbiaya,CASE WHEN biayagantinama>0 THEN biayagantinama ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'biayaprosestutup' AS parameter,'biayaprosestutup' AS postbiaya,CASE WHEN biayaprosestutup>0 THEN biayaprosestutup ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'biayaprosesbuka' AS parameter,'biayaprosesbuka' AS postbiaya,CASE WHEN biayaprosesbuka>0 THEN biayaprosesbuka ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'dendapelanggaran' AS parameter,'dendapelanggaran' AS postbiaya,CASE WHEN dendapelanggaran>0 THEN dendapelanggaran ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'jasadaribahan' AS parameter,'jasadaribahan' AS postbiaya,CASE WHEN jasadaribahan>0 THEN jasadaribahan ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'penyesuaian' AS parameter,'penyesuaian' AS postbiaya,CASE WHEN penyesuaian>0 THEN penyesuaian ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'lainnya' AS parameter,'lainnya' AS postbiaya,CASE WHEN lainnya>0 THEN lainnya ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
UNION ALL SELECT b.id,'realisasisambungan' AS parameter,'lainnya' AS postbiaya,CASE WHEN realisasisambungan>0 THEN realisasisambungan ELSE NULL END AS `value`, a.waktuupdate FROM __tmp_nonair b JOIN [table] a ON a.urutan=b.urutan WHERE a.periode=@periode OR a.periode IS NULL OR a.periode=''
) p
WHERE p.value IS NOT NULL;