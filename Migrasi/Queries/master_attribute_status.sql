DROP TABLE IF EXISTS _status;

CREATE TABLE _status (
 idpdam SMALLINT (6) NOT NULL,
 idstatus INT (11) NOT NULL,
 namastatus VARCHAR (50),
 rekening_air_include TINYINT (1) DEFAULT 0,
 rekening_limbah_include TINYINT (1) DEFAULT 0,
 rekening_lltt_include TINYINT (1) DEFAULT 0,
 tanpabiayapemakaianair TINYINT (1) DEFAULT 0,
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idstatus)
) ENGINE = INNODB;

INSERT INTO _status(idpdam,idstatus,namastatus,rekening_air_include) VALUES
(@idpdam,-1,'Calon Pelanggan',0),
(@idpdam,0,'Non Aktif',0),
(@idpdam,1,'Aktif',1),
(@idpdam,2,'Segel',1),
(@idpdam,3,'Tutup Sementara',1),
(@idpdam,4,'Blokir',0);

SELECT
 idpdam,
 idstatus,
 namastatus,
 rekening_air_include,
 rekening_limbah_include,
 rekening_lltt_include,
 tanpabiayapemakaianair,
 flaghapus,
 waktuupdate
FROM
 _status
 WHERE idpdam=@idpdam;
 
DROP TABLE _status;