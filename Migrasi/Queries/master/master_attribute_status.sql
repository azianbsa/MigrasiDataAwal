-- master_attribute_status
-- new(0, "idpdam")
-- new(1, "idstatus")
-- new(2, "namastatus")
-- new(3, "rekening_air_include")
-- new(4, "rekening_limbah_include")
-- new(5, "rekening_lltt_include")
-- new(6, "tanpabiayapemakaianair")
-- new(7, "flaghapus")
-- new(8, "waktuupdate")

DROP TABLE IF EXISTS __tmp_status;

CREATE TABLE __tmp_status (
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

INSERT INTO __tmp_status(idpdam,idstatus,namastatus,rekening_air_include) VALUES
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
 __tmp_status
 WHERE idpdam=@idpdam;
 
DROP TABLE __tmp_status;