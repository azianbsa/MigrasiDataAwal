﻿SET FOREIGN_KEY_CHECKS=0;

INSERT INTO `master_pdam` (idpdam, `namapdam`, tipe)
VALUES
(@idpdam, @namapdam, 'basic');

INSERT INTO `master_pdam_connection`
SELECT
@idpdam,
`db_host`,
`db_port`,
`db_user`,
`db_password`,
`db_name`,
`toppi_config_id`,
@timezone AS `timezone`,
`storageservice`,
`flagaktif`,
`userstamp`,
`passwordstamp`,
`show_warning_pembayaran`,
`duration_warning_pembayaran`,
`disable_penerbitan_tagihan`,
`waktuupdate`
FROM
`master_pdam_connection` WHERE idpdam=@idpdamcopy;

INSERT INTO `master_role` (idpdam,idrole,`namarole`)
VALUES(@idpdam,1,'Developer');

INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,1,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,2,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,3,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,4,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,5,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,6,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,7,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,8,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,9,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,10,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,11,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,12,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,13,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,14,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,15,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,16,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,17,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,18,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,19,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,20,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,21,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,22,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,23,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,24,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,25,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,26,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,27,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,28,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,29,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,30,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,31,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,32,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,33,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,34,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,35,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,36,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,37,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,38,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,39,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,40,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,41,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,42,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,43,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,45,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,46,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,47,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,48,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,49,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,50,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,51,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,52,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,53,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,54,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,55,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,56,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,57,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,58,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,59,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,60,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,61,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,62,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,63,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,64,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,65,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,66,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,67,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,68,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,69,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,71,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,72,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,73,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,74,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,75,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,76,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,77,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,78,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,79,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,80,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,81,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,82,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,83,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,84,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,85,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,86,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,87,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,88,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,89,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,90,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,91,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,92,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,93,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,94,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,95,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,96,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,97,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,98,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,99,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,100,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,101,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,102,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,103,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,104,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,105,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,106,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,107,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,108,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,109,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,110,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,111,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,115,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,116,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,117,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,118,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,119,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,120,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,121,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,122,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,123,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,124,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,125,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,126,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,128,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,129,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,130,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,131,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,132,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,133,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,134,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,135,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,136,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,137,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,138,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,139,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,140,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,141,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,142,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,143,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,144,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,145,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,146,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,147,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,148,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,149,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,150,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,151,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,152,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,153,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,154,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,155,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,156,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,157,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,158,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,159,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,160,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,161,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,162,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,163,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,164,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,166,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,167,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,168,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,169,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,170,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,171,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,172,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,173,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,174,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,175,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,176,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,177,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,178,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,179,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,180,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,181,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,182,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,183,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,184,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,185,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,186,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,187,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,188,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,189,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,190,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,191,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,192,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,193,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,194,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,195,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,196,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,197,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,198,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,199,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,200,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,201,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,202,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,203,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,204,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,205,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,206,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,207,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,208,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,209,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,210,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,211,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,212,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,213,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,214,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,215,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,216,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,217,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,218,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,219,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,220,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,221,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,222,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,223,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,224,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,225,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,226,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,227,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,228,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,229,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,230,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,231,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,232,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,233,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,234,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,235,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,236,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,237,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,238,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,239,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,242,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,243,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,244,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,245,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,246,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,247,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,248,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,368,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,369,1,0,NOW());
INSERT INTO `master_role_access` (`idpdam`,`idrole`,`idmoduleaccess`,`value`,`flaghapus`,`waktuupdate`) VALUES (@idpdam,1,370,1,0,NOW());

INSERT INTO setting_gcs
SELECT
@idpdam,
idgcssetting,
credential,
bucket,
CONCAT(SUBSTRING_INDEX(fotometerpath,'/',1),'/',@idpdam,'-',LOWER(REPLACE(@namapdam,' ','-')),'/'),
flagaktif,
flaghapus,
waktuupdate
FROM
setting_gcs
WHERE idpdam=@idpdamcopy;

SET FOREIGN_KEY_CHECKS=0;