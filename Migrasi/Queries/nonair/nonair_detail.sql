SET @tgl_ent_awal='2025-06-27';
SET @tgl_ent_akhir='2025-06-30';

SELECT
@idpdam AS idpdam,
a.`idnonair` idnonair,
a.parameter AS parameter,
a.postbiaya AS postbiaya,
a.value AS `value`,
a.`tgl_ent` AS waktuupdate
FROM (
SELECT b.`idnonair`,'pendaftaran' AS parameter,'pendaftaran' AS postbiaya,`pdp_reg` AS `value`,a.`tgl_ent`
FROM `t_jurair` a JOIN `migrasi_nonair` b ON b.`no_reg`=a.`no_reg` AND b.`no_byr`=a.`no_byr` AND b.`jns`=a.`jns`
WHERE a.`tgl_ent`>=@tgl_ent_awal AND a.`tgl_ent`<@tgl_ent_akhir
UNION ALL
SELECT b.`idnonair`,'administrasi' AS parameter,'administrasi' AS postbiaya,`pdp_adm` AS `value`,a.`tgl_ent`
FROM `t_jurair` a JOIN `migrasi_nonair` b ON b.`no_reg`=a.`no_reg` AND b.`no_byr`=a.`no_byr` AND b.`jns`=a.`jns`
WHERE a.`tgl_ent`>=@tgl_ent_awal AND a.`tgl_ent`<@tgl_ent_akhir
UNION ALL
SELECT b.`idnonair`,'biayapasang' AS parameter,'biayapasang' AS postbiaya,`pdp_smb` AS `value`,a.`tgl_ent`
FROM `t_jurair` a JOIN `migrasi_nonair` b ON b.`no_reg`=a.`no_reg` AND b.`no_byr`=a.`no_byr` AND b.`jns`=a.`jns`
WHERE a.`tgl_ent`>=@tgl_ent_awal AND a.`tgl_ent`<@tgl_ent_akhir
UNION ALL
SELECT b.`idnonair`,'lainnya' AS parameter,'lainnya' AS postbiaya,`pdp_nairl`+`pdp_t` AS `value`,a.`tgl_ent`
FROM `t_jurair` a JOIN `migrasi_nonair` b ON b.`no_reg`=a.`no_reg` AND b.`no_byr`=a.`no_byr` AND b.`jns`=a.`jns`
WHERE a.`tgl_ent`>=@tgl_ent_awal AND a.`tgl_ent`<@tgl_ent_akhir
UNION ALL
SELECT b.`idnonair`,'ppn' AS parameter,'ppn' AS postbiaya,`pdp_ppn` AS `value`,a.`tgl_ent`
FROM `t_jurair` a JOIN `migrasi_nonair` b ON b.`no_reg`=a.`no_reg` AND b.`no_byr`=a.`no_byr` AND b.`jns`=a.`jns`
WHERE a.`tgl_ent`>=@tgl_ent_awal AND a.`tgl_ent`<@tgl_ent_akhir
) a