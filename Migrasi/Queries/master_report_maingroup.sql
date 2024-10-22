SELECT
 IdReportMainGroup,
 @idpdam,
 Nama,
 FlagHapus,
 WaktuUpdate
FROM
 master_report_maingroup
WHERE idpdam=@idpdamcopy