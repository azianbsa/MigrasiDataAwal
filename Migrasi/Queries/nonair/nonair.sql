SET @tgl_ent_awal='2023-01-01';
SET @tgl_ent_akhir='2025-06-01';

SELECT 
@idpdam AS idpdam,
b.`idnonair` AS idnonair,
c.`idjenisnonair` AS idjenisnonair,
p.`idpelangganair` AS idpelangganair,
NULL AS idpelangganlimbah,
NULL AS idpelangganlltt,
IF(a.`no_byr`='-',NULL,DATE_FORMAT(a.`tgl_byr`,'%Y%m')) AS kodeperiode,
b.`nomornonair` AS nomornonair,
a.`ket` AS keterangan,
a.`jmlbyr` AS total,
a.`tgl_ent` AS tanggalmulaitagih,
NULL AS tanggalkadaluarsa,
a.`nama` AS nama,
a.`alamat` AS alamat,
NULL AS idrayon,
NULL AS idkelurahan,
NULL AS idgolongan,
NULL AS idtariflimbah,
NULL AS idtariflltt,
0 AS flagangsur,
NULL AS idangsuran,
0 AS termin,
CASE WHEN a.`no_reg`='-' OR a.`no_reg`='--' OR a.`no_reg`='_' THEN 1 ELSE 0 END AS flagmanual,
NULL AS idpermohonansambunganbaru,
0 AS flaghapus,
um.`iduser` AS iduser,
a.`tgl_ent` AS waktuupdate,
a.`tgl_ent` AS created_at
FROM `t_jurair` a
JOIN `migrasi_nonair` b ON b.`no_reg`=a.`no_reg` AND b.`no_byr`=a.`no_byr` AND b.`jns`=a.`jns` 
JOIN `jenismaros` c ON c.`kodejenisnonair`=b.`jns2` AND c.flaghapus=0
LEFT JOIN `t_user` u ON u.`NO_ID`=a.`opr`
LEFT JOIN `usermaros` um ON um.`nama`=u.`NAMA_USER`
LEFT JOIN `pelangganmaros` p ON p.`nosamb`=a.`nosamb`
WHERE a.`tgl_ent`>=@tgl_ent_awal
AND a.`tgl_ent`<@tgl_ent_akhir