﻿SELECT * FROM dashboard_user_access WHERE iduser = (SELECT id FROM dashboard_user WHERE username = 'admin')