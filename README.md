# clickhouse_backup_rename
Сырой образец.
Как пользоваться 
1. Сделать бекап посредством clickhouse-backup
2. Сбилдить бинарь, или в мейкфайле 
- - -d MyOldDB:myNewDB -t old_events:my_new_events backName
заменить на свои данные
Возможно 
1. -d MyOldDB:myNewDB backName :- копирует все данные в новую БД.
2. -d MyOldDB:myNewDB -t old_events backName :- копирует одну таблицу в новую БД.
3. -d MyOldDB -t old_events:my_new_events backName копирует таблицу изменяя имя.
4. -d MyOldDB:myNewDB -t old_events:my_new_events backName :- Копирует старую из старой БД в новую переименовуя старую таблицу в новую.
