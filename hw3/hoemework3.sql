SELECT ID_тикета, 
    GROUP_CONCAT(CONCAT(Status_time, " " , Статус, " ", Группа) ORDER BY status_time SEPARATOR '; ') Назначение
    FROM sem3tab_02
    GROUP BY ID_тикета
