/* rekening_nonair_detail
 * new(0, "idpdam")
 * new(1, "idnonair")
 * new(2, "parameter")
 * new(3, "postbiaya")
 * new(4, "value")
 * new(5, "waktuupdate")
 */

SELECT
@idpdam AS idpdam,
p.idnonair AS idnonair,
p.parameter AS parameter,
p.postbiaya AS postbiaya,
p.value AS `value`,
p.waktuupdate AS waktuupdate
FROM (
SELECT b.idnonair,'biayapemakaian' AS parameter,'biayapemakaian' AS postbiaya,CASE WHEN biayapemakaian>0 THEN biayapemakaian ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'administrasi' AS parameter,'administrasi' AS postbiaya,CASE WHEN administrasi>0 THEN administrasi ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'pemeliharaan' AS parameter,'pemeliharaan' AS postbiaya,CASE WHEN pemeliharaan>0 THEN pemeliharaan ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'retribusi' AS parameter,'retribusi' AS postbiaya,CASE WHEN retribusi>0 THEN retribusi ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'dendatunggakan' AS parameter,'dendatunggakan' AS postbiaya,CASE WHEN dendatunggakan>0 THEN dendatunggakan ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'ppn' AS parameter,'ppn' AS postbiaya,CASE WHEN ppn>0 THEN ppn ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'biayabahan' AS parameter,'biayabahan' AS postbiaya,CASE WHEN biayabahan>0 THEN biayabahan ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'biayapasang' AS parameter,'biayapasang' AS postbiaya,CASE WHEN biayapasang>0 THEN biayapasang ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'pendaftaran' AS parameter,'pendaftaran' AS postbiaya,CASE WHEN pendaftaran>0 THEN pendaftaran ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'jaminan' AS parameter,'jaminan' AS postbiaya,CASE WHEN jaminan>0 THEN jaminan ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'perencanaan' AS parameter,'perencanaan' AS postbiaya,CASE WHEN perencanaan>0 THEN perencanaan ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'meterai' AS parameter,'meterai' AS postbiaya,CASE WHEN meterai>0 THEN meterai ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'tangki' AS parameter,'tangki' AS postbiaya,CASE WHEN tangki>0 THEN tangki ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'perbaikan' AS parameter,'perbaikan' AS postbiaya,CASE WHEN perbaikan>0 THEN perbaikan ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'biayagantinama' AS parameter,'biayagantinama' AS postbiaya,CASE WHEN biayagantinama>0 THEN biayagantinama ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'biayaprosestutup' AS parameter,'biayaprosestutup' AS postbiaya,CASE WHEN biayaprosestutup>0 THEN biayaprosestutup ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'biayaprosesbuka' AS parameter,'biayaprosesbuka' AS postbiaya,CASE WHEN biayaprosesbuka>0 THEN biayaprosesbuka ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'dendapelanggaran' AS parameter,'dendapelanggaran' AS postbiaya,CASE WHEN dendapelanggaran>0 THEN dendapelanggaran ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'jasadaribahan' AS parameter,'jasadaribahan' AS postbiaya,CASE WHEN jasadaribahan>0 THEN jasadaribahan ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'penyesuaian' AS parameter,'penyesuaian' AS postbiaya,CASE WHEN penyesuaian>0 THEN penyesuaian ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'lainnya' AS parameter,'lainnya' AS postbiaya,CASE WHEN lainnya>0 THEN lainnya ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
UNION ALL SELECT b.idnonair,'realisasisambungan' AS parameter,'lainnya' AS postbiaya,CASE WHEN realisasisambungan>0 THEN realisasisambungan ELSE NULL END AS `value`, a.waktuupdate FROM nonair a JOIN [dataawal].`tampung_rekening_nonair` b ON b.urutan=a.urutan AND b.idpdam=@idpdam
) p
WHERE p.value IS NOT NULL;