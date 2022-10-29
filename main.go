package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	cp "github.com/otiai10/copy"
)

type TableData struct {
	DbName    string
	Tablename string
	UUID      string
}
type GetInitData struct {
	OldDBName    string
	OldTableName string
	NewDBName    string
	NewTableName string
	Backup       string
}
type UUID struct {
	shortOldUUID string
	shortNewUUID string
	oldUUID      string
	newUUID      string
}
type FilePath struct {
	MetaDir        string
	MetaDBDir      string
	ShadowDir      string
	ShadowDBDir    string
	ShadowTableDir string
}

var (
	generalData   TableData
	initData      GetInitData
	uuidData      UUID
	payload       map[string]interface{}
	filePath      FilePath
	userDataOwner string = "efremov"

	// app         = kingpin.New("main", "Enter your Database!")
	// databaseOld = app.Arg("OldDB", "Old DB").Required().String()
	// databaseNew = app.Arg("NewDB", "New DB").Required().String()
)

func main() {
	// kingpin.MustParse(app.Parse(os.Args))

	flagMain()
	getData(initData.Backup, initData.OldDBName, initData.OldTableName)
}

func flagMain() {
	dbFlag := flag.String("d", "", "Use Flag: main -d OldDbName:NewDbName <backup_name>")
	tableFlag := flag.String("t", "", "Use Flag: main example -d OldDbName:NewDbName [-t OldTableName:NewTableName] <backup_name>")
	var (
		sTb []string
		sDb []string
	)
	flag.Parse()
	if len(flag.Args()) <= 0 {
		log.Fatal("\nUse: main -d string -t string <backup_name>")
		return
	}
	if *dbFlag != "" {
		sDb = strings.Split(*dbFlag, ":")
		if len(sDb) <= 1 {
			sDb[0] = *dbFlag
			fmt.Println(sDb)
		}
	} else {
		log.Fatal("You need to select your DataBase!")
	}
	// если нет талицы задаём только имена БД.
	// если есть талицы тогда сплитим её.
	if *tableFlag == "" {
		initData = GetInitData{OldDBName: sDb[0], NewDBName: sDb[1], Backup: flag.Args()[0]}
		return
	} else {
		sTb = strings.Split(*tableFlag, ":")
	}
	// НУЖНО СОЗДАТЬ УСЛОВИЕ КОГДА КОПИРУЕМ ОДНУ ТАБЛИЦУ В НОВУЮ БД
	if len(sDb) >= 2 {
		initData = GetInitData{OldDBName: sDb[0], NewDBName: sDb[1], OldTableName: sTb[0], NewTableName: sTb[1], Backup: flag.Args()[0]}
		return
	} else if len(sDb) <= 1 {
		initData = GetInitData{OldDBName: sDb[0], OldTableName: sTb[0], NewTableName: sTb[1], Backup: flag.Args()[0]}
		return
	}
	log.Fatal("Use 'main -d string -t string <backup_name>'")
	// fmt.Println(initData.OldDBName, initData.OldTableName, initData.NewDBName, initData.NewTableName, initData.Backup)
}

func regUUID(Meta string) {
	// Здесть я создаю новый UUID на основе старого, для предотвращения создания множества директорий в /storage/ и изменяю Мету.

	r, _ := regexp.Compile(`((\d)|(\w)){8}-((\d)|(\w)){4}-((\d)|(\w)){4}-((\d)|(\w)){4}-((\d)|(\w)){12}`)
	newID := uuid.New().String()
	uuidData = UUID{shortOldUUID: r.FindString(generalData.UUID)[:3], oldUUID: r.FindString(generalData.UUID), shortNewUUID: newID[:3], newUUID: newID}
	uuidData.newUUID = strings.Replace(uuidData.newUUID, uuidData.shortNewUUID, uuidData.shortOldUUID, -1)
	replacer := strings.NewReplacer(uuidData.oldUUID, uuidData.newUUID, generalData.DbName, initData.NewDBName, generalData.Tablename, initData.NewTableName)
	payload["query"] = replacer.Replace(payload["query"].(string))
	payload["database"] = initData.NewDBName
	payload["table"] = initData.NewTableName
	writeMeta(payload, Meta)
}

func writeMeta(File map[string]interface{}, Meta string) {
	// Оставить просто функцию без условий, условия будут на предыдущем шаге.
	jsonString, err := json.MarshalIndent(File, "   ", " ")
	if err != nil {
		log.Fatal("JSON marshaling failed:", err)
	}
	// Если нет новой бд, тогда создаем таблицу в той-же базе данных (одну таблицу копируем)
	// Если установлена мета(список всех таблиц), тогда копируем в новую БД все таблицы
	// Если нет меты(списка таблиц), тогда копируем одну таблицу в Новую БД
	// НУЖНО СОЗДАТЬ УСЛОВИЕ КОГДА КОПИРУЕМ ОДНУ ТАБЛИЦУ В НОВУЮ БД
	if initData.NewDBName == "" {
		ioutil.WriteFile(filePath.MetaDBDir+initData.NewTableName+".json", jsonString, 0644)
		userChown(filePath.MetaDBDir + initData.NewTableName + ".json")
	} else if Meta != "" {
		ioutil.WriteFile(filePath.MetaDir+initData.NewDBName+"/"+Meta, jsonString, 0644)
		userChown(filePath.MetaDir + initData.NewDBName + "/" + Meta)
	} else {
		ioutil.WriteFile(filePath.MetaDir+initData.NewDBName+"/"+initData.NewTableName+".json", jsonString, 0644)
		userChown(filePath.MetaDir + initData.NewDBName + "/" + initData.NewTableName + ".json")
	}
}

// Получаю данные, создаю Мету.
func getData(backupName string, dbName string, tableName string) {
	// mainDir := "/var/lib/clickhouse/backup/" + backupName
	mainDir := "/home/efremov/myprojecs/golang/clickhouse/backup/" + backupName
	filePath = FilePath{
		MetaDir:        mainDir + "/metadata/",
		MetaDBDir:      mainDir + "/metadata/" + dbName + "/",
		ShadowDir:      mainDir + "/shadow/",
		ShadowDBDir:    mainDir + "/shadow/" + dbName + "/",
		ShadowTableDir: mainDir + "/shadow/" + dbName + "/" + tableName}
	// здесь немного сокращаю код. Создаю Директории.
	// Если нет меты(таблицы), тогда копируем польностью БД (со старыми таблицами)
	// Если таблицы есть,но нет новой БД значит мету не трогаем (создаём в старой БД)
	// Или содаём новую бд с новой метой.(таблицами)
	if tableName == "" {
		createDir(filePath.MetaDir+initData.NewDBName, filePath.ShadowDir+initData.NewDBName)
		allTables(filePath.MetaDBDir)
		fmt.Printf("Use all Tables %s of\n%s\n", dbName, backupName)
	} else if initData.NewDBName == "" {
		createDir("", filePath.ShadowDBDir+initData.NewTableName)
		oneTable(filePath.MetaDBDir + tableName)
	} else {
		createDir(filePath.MetaDir+initData.NewDBName, filePath.ShadowDir+initData.NewDBName+"/"+initData.NewTableName)
		oneTable(filePath.MetaDBDir + tableName)
	}
}

// func renameDBDir(srcM string, dstM string, srcS string, dstS string) {
// 	fmt.Println(filePath.ShadowTableDir, filePath.ShadowDir+initData.NewDBName+"/"+initData.NewTableName)
// 	err := os.Rename(srcM, dstM)
// 	if err != nil {
// 		fmt.Println(err)
// 		return
// 	}
// 	err2 := os.Rename(srcS, dstS)
// 	if err2 != nil {
// 		fmt.Println(err2, "firces")
// 		return
// 	}
// 	// if len(initData.NewTableName) != 0 {
// 	err3 := os.Rename(dstS+"/"+initData.OldTableName, filePath.ShadowDir+initData.NewDBName+"/"+initData.NewTableName)
// 	if err3 != nil {
// 		fmt.Println(err3)
// 		return
// 	}
// 	// }
// }

func allTables(metaDir string) {
	jsonfiles, err := ioutil.ReadDir(metaDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range jsonfiles {
		jsonMain, _ := ioutil.ReadFile(metaDir + file.Name())
		if err != nil {
			log.Fatal("Error when opening file: ", err)
		}

		err = json.Unmarshal(jsonMain, &payload)
		if err != nil {
			log.Fatal("Error during Unmarshal(): ", err)
		}

		generalData = TableData{UUID: payload["query"].(string), DbName: payload["database"].(string), Tablename: payload["table"].(string)}
		regUUID(file.Name())
	}
	copyShadow(filePath.ShadowDBDir, filePath.ShadowDir+initData.NewDBName)
	// fmt.Printf("UUID: %s\nDbName: %s\nTableName: %s\n", generalData.UUID, generalData.DbName, generalData.Tablename)
}

func oneTable(metaTables string) {
	jsonMain, err := ioutil.ReadFile(metaTables + ".json")
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}

	err = json.Unmarshal(jsonMain, &payload)
	if err != nil {
		log.Fatal("Error during Unmarshal(): ", err)
	}

	generalData = TableData{UUID: payload["query"].(string), DbName: payload["database"].(string), Tablename: payload["table"].(string)}
	regUUID("")
	fmt.Printf("UUID: %s\nDbName: %s\nTableName: %s\n", generalData.UUID, generalData.DbName, generalData.Tablename)
	copyShadow(filePath.ShadowTableDir, filePath.ShadowDir+initData.NewDBName+"/"+initData.OldTableName)

}
func createDir(Meta, Shadow string) {
	if Meta == "" {
	} else {
		if _, err := os.Stat(Meta); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(Meta, os.ModePerm)
			if err != nil {
				log.Fatal(err)
			}
		}
		userChown(Meta)
	}
	if _, err := os.Stat(Shadow); errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(Shadow, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
		userChown(Shadow)
	}
}

func userChown(File string) {
	userD, err := user.Lookup(userDataOwner)
	if err != nil {
		return
	}
	GidStr, _ := userD.GroupIds()
	Gid, _ := strconv.Atoi(GidStr[0])
	Uid, _ := strconv.Atoi(*&userD.Uid)
	err = os.Chown(File, Uid, Gid)
	if err != nil {
		fmt.Println(err)
	}

}

func copyShadow(srcDir, dest string) {
	err := cp.Copy(srcDir, dest)
	fmt.Println(err) // nil
	fmt.Println(dest)
}
