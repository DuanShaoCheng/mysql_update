# mysql_update
让mysql库应用sql的结构文件,在不清除所有数据的情况下应用表结构的变更
## 准备工作
- 需要py 3.x 版本的python
- 应用 requirements.txt 中的python库 pip install -r requirements.txt
- 修改配置文件。
- 配置文件中配置的数据库必须有修改创建数据库、修改表结构的权限，最好使用root用户。
- 配置文件中的 dbName 对应的数据库必须存在。
- 执行前做好数据备份，不保证一定可以执行成功。
## 原理
- 会在配置的数据库连接中创建一个新的数据库，名字为 dbName 加 “_new” 然后执行配置的sqlDir目录下的所有sql文件。
- 对dbName 与 dbName_new 的数据库中的表做对比，多的删，少的加，不一致的更新，生成对应的sql语句。
- 对dbName 数据库应用对比生成的sql语句，实现对表结构的变更。