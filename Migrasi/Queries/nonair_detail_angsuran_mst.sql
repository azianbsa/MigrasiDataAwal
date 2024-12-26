SELECT
@idpdam,
p.id AS idnonair,
'Biaya' AS parameter,
p.postbiaya AS postbiaya,
p.value AS `value`,
NOW() AS waktuupdate
FROM (
SELECT id,'biayapemakaian' AS postbiaya,CASE WHEN biayapemakaian>0 THEN biayapemakaian ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'administrasi' AS postbiaya,CASE WHEN administrasi>0 THEN administrasi ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'pemeliharaan' AS postbiaya,CASE WHEN pemeliharaan>0 THEN pemeliharaan ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'retribusi' AS postbiaya,CASE WHEN retribusi>0 THEN retribusi ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'dendatunggakan' AS postbiaya,CASE WHEN dendatunggakan>0 THEN dendatunggakan ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'ppn' AS postbiaya,CASE WHEN ppn>0 THEN ppn ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'biayabahan' AS postbiaya,CASE WHEN biayabahan>0 THEN biayabahan ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'biayapasang' AS postbiaya,CASE WHEN biayapasang>0 THEN biayapasang ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'pendaftaran' AS postbiaya,CASE WHEN pendaftaran>0 THEN pendaftaran ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'jaminan' AS postbiaya,CASE WHEN jaminan>0 THEN jaminan ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'perencanaan' AS postbiaya,CASE WHEN perencanaan>0 THEN perencanaan ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'meterai' AS postbiaya,CASE WHEN meterai>0 THEN meterai ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'tangki' AS postbiaya,CASE WHEN tangki>0 THEN tangki ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'perbaikan' AS postbiaya,CASE WHEN perbaikan>0 THEN perbaikan ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'biayagantinama' AS postbiaya,CASE WHEN biayagantinama>0 THEN biayagantinama ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'biayaprosestutup' AS postbiaya,CASE WHEN biayaprosestutup>0 THEN biayaprosestutup ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'biayaprosesbuka' AS postbiaya,CASE WHEN biayaprosesbuka>0 THEN biayaprosesbuka ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'dendapelanggaran' AS postbiaya,CASE WHEN dendapelanggaran>0 THEN dendapelanggaran ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
UNION ALL SELECT id,'lainnya' AS postbiaya,CASE WHEN lainnya>0 THEN lainnya ELSE NULL END AS `value` FROM nonair WHERE flagangsur = 1 AND flaghapus = 1 AND termin = 0 AND ketjenis NOT LIKE 'Uang_Muka%'
) p
WHERE p.value IS NOT NULL;