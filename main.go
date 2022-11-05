package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
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
	userDataOwner string = "clickhouse"

	// app         = kingpin.New("main", "Enter your Database!")
	// databaseOld = app.Arg("OldDB", "Old DB").Required().String()
	// databaseNew = app.Arg("NewDB", "New DB").Required().String()
)

func main() {
	flagMain()
	getData(initData.Backup, initData.OldDBName, initData.OldTableName)
	distributor()
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
		// return err
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
		if len(sTb) <= 1 {
			sTb[0] = *tableFlag
			fmt.Println(sTb)
		}
	}
	// НУЖНО СОЗДАТЬ УСЛОВИЕ КОГДА КОПИРУЕМ ОДНУ ТАБЛИЦУ В НОВУЮ БД
	if len(sDb) >= 2 {
		if len(sTb) >= 2 {
			initData = GetInitData{OldDBName: sDb[0], NewDBName: sDb[1], OldTableName: sTb[0], NewTableName: sTb[1], Backup: flag.Args()[0]}
			return
		} else {
			initData = GetInitData{OldDBName: sDb[0], NewDBName: sDb[1], OldTableName: sTb[0], Backup: flag.Args()[0]}
			return
		}
	} else if len(sDb) <= 1 {
		initData = GetInitData{OldDBName: sDb[0], OldTableName: sTb[0], NewTableName: sTb[1], Backup: flag.Args()[0]}
		return
	}
	log.Fatal("Use 'main -d string:string -t string:string <backup_name>'")
	// fmt.Println(initData.OldDBName, initData.OldTableName, initData.NewDBName, initData.NewTableName, initData.Backup)
}

// Получаю данные, создаю Мету.
func getData(backupName string, dbName string, tableName string) {
	mainDir := "/var/lib/clickhouse/backup/" + backupName
	// mainDir := "/home/efremov/myprojecs/golang/clickhouse/backup/" + backupName
	filePath = FilePath{
		MetaDir:        mainDir + "/metadata/",
		MetaDBDir:      mainDir + "/metadata/" + dbName + "/",
		ShadowDir:      mainDir + "/shadow/",
		ShadowDBDir:    mainDir + "/shadow/" + dbName + "/",
		ShadowTableDir: mainDir + "/shadow/" + dbName + "/" + tableName}
	fmt.Printf("getData\n")
}

func distributor() {
	// здесь немного сокращаю код. Создаю Директории.
	// Если нет меты(таблицы), тогда копируем польностью БД (со старыми таблицами)
	// Если таблицы есть,но нет новой БД значит мету не трогаем (создаём в старой БД)
	// Или содаём новую бд с новой метой.(таблицами)
	if initData.OldTableName == "" {
		// Если нет таблиц, копирую всю базу, с одинаковыми названиями таблиц.
		createDir(filePath.MetaDir+initData.NewDBName, filePath.ShadowDir+initData.NewDBName)
		allTables(filePath.MetaDBDir)
		fmt.Printf("Use all Tables %s of\n%s\n", initData.OldDBName, initData.Backup)
	} else if initData.NewDBName == "" {
		// Если нет новой бд, тогда создаем таблицу в той-же базе данных (одну таблицу копируем)
		createDir("", filePath.ShadowDBDir+initData.NewTableName)
		oneTable(filePath.MetaDBDir + initData.OldTableName)
	} else if initData.NewTableName != "" {
		createDir(filePath.MetaDir+initData.NewDBName, filePath.ShadowDir+initData.NewDBName+"/"+initData.NewTableName)
		oneTableNewDB(filePath.MetaDBDir + initData.OldTableName)
		// Производим замену UUID/database/table во всей мете
		replacer := strings.NewReplacer(generalData.DbName, initData.NewDBName, uuidData.oldUUID, uuidData.newUUID, generalData.Tablename, initData.NewTableName)
		payload["table"] = initData.NewTableName
		payload["database"] = initData.NewDBName
		payload["query"] = replacer.Replace(payload["query"].(string))
		writeMeta(payload, filePath.MetaDir+initData.NewDBName+"/"+initData.NewTableName+".json")
		copyDir(filePath.ShadowTableDir, filePath.ShadowDir+initData.NewDBName+"/"+initData.NewTableName)
		userChown(filePath.ShadowDir + initData.NewDBName)
		fmt.Printf("oneTableNewDB\n")
	} else {
		createDir(filePath.MetaDir+initData.NewDBName, filePath.ShadowDir+initData.NewDBName+"/"+initData.OldTableName)
		oneTableNewDB(filePath.MetaDBDir + initData.OldTableName)
		// Производим замену UUID/database/table во всей мете
		replacer := strings.NewReplacer(generalData.DbName, initData.NewDBName, uuidData.oldUUID, uuidData.newUUID)
		payload["database"] = initData.NewDBName
		payload["query"] = replacer.Replace(payload["query"].(string))
		writeMeta(payload, filePath.MetaDir+initData.NewDBName+"/"+initData.OldTableName+".json")
		copyDir(filePath.ShadowTableDir, filePath.ShadowDir+initData.NewDBName+"/"+initData.OldTableName)
		userChown(filePath.ShadowDir + initData.NewDBName)
		fmt.Printf("oneTableNewDB\n")
	}
}

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
		generalData = TableData{UUID: payload["query"].(string), DbName: payload["database"].(string)}
		regUUID()
		// Производим замену UUID/database во всей мете
		replacer := strings.NewReplacer(generalData.DbName, initData.NewDBName)
		payload["query"] = replacer.Replace(payload["query"].(string))
		payload["database"] = initData.NewDBName
		writeMeta(payload, filePath.MetaDir+initData.NewDBName+"/"+file.Name())

	}
	copyDir(filePath.ShadowDBDir, filePath.ShadowDir+initData.NewDBName)
	fmt.Printf("TABLE: %s.", payload["table"].(string))
}
func oneTableNewDB(metaTables string) {
	jsonMain, err := ioutil.ReadFile(metaTables + ".json")
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}
	err = json.Unmarshal(jsonMain, &payload)
	if err != nil {
		log.Fatal("Error during Unmarshal(): ", err)
	}
	generalData = TableData{UUID: payload["query"].(string), DbName: payload["database"].(string), Tablename: payload["table"].(string)}
	regUUID()

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
	regUUID()
	// Производим замену UUID/table во всей мете
	replacer := strings.NewReplacer(uuidData.oldUUID, uuidData.newUUID, generalData.Tablename, initData.NewTableName)
	payload["table"] = initData.NewTableName
	payload["query"] = replacer.Replace(payload["query"].(string))
	writeMeta(payload, filePath.MetaDBDir+initData.NewTableName+".json")
	copyDir(filePath.ShadowTableDir, filePath.ShadowDBDir+initData.NewTableName)
	fmt.Printf("oneTable\n")

}

func regUUID() {
	// Здесть я создаю новый UUID на основе старого, для предотвращения создания множества директорий в /storage/ и изменяю Мету.
	r, _ := regexp.Compile(`((\d)|(\w)){8}-((\d)|(\w)){4}-((\d)|(\w)){4}-((\d)|(\w)){4}-((\d)|(\w)){12}`)
	newID := uuid.New().String()
	// Если UUID reqexp забрал, тогда его изменяем.
	if len(r.FindString(generalData.UUID)) > 0 {
		uuidData = UUID{shortOldUUID: r.FindString(generalData.UUID)[:3], oldUUID: r.FindString(generalData.UUID), shortNewUUID: newID[:3], newUUID: newID}
		uuidData.newUUID = strings.Replace(uuidData.newUUID, uuidData.shortNewUUID, uuidData.shortOldUUID, -1)
	}
}

func writeMeta(File map[string]interface{}, Path string) {
	// Оставить просто функцию без условий, условия будут на предыдущем шаге.
	jsonString, err := json.MarshalIndent(File, "   ", " ")
	if err != nil {
		log.Fatal("JSON marshaling failed:", err)
	}
	err = ioutil.WriteFile(Path, jsonString, 0644)
	if err != nil {
		log.Fatal("JSON marshaling failed WriteFile:", err)
	}
	userChown(Path)
	fmt.Printf("writeMeta\n")
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
	fmt.Printf("ucreateDir\n")
}

func userChown(File string) error {
	userD, err := user.Lookup(userDataOwner)
	if err != nil {
		return err
	}
	GidStr, _ := userD.GroupIds()
	Gid, _ := strconv.Atoi(GidStr[0])
	Uid, _ := strconv.Atoi(*&userD.Uid)
	err = os.Chown(File, Uid, Gid)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func copyDir(src string, dst string) error {
	var err error
	var fds []os.FileInfo
	var srcinfo os.FileInfo

	if srcinfo, err = os.Stat(src); err != nil {
		return err
	}

	if err = os.MkdirAll(dst, srcinfo.Mode()); err != nil {
		return err
	}
	userChown(dst)

	if fds, err = ioutil.ReadDir(src); err != nil {
		return err
	}
	for _, fd := range fds {
		srcfp := path.Join(src, fd.Name())
		dstfp := path.Join(dst, fd.Name())

		if fd.IsDir() {
			if err = copyDir(srcfp, dstfp); err != nil {
				fmt.Println(err)
			}
		} else {
			if err = copyFile(srcfp, dstfp); err != nil {
				fmt.Println(err)
			}
		}
		userChown(dstfp)
	}
	return nil
}

func copyFile(src, dst string) error {
	var err error
	var srcfd *os.File
	var dstfd *os.File
	var srcinfo os.FileInfo

	if srcfd, err = os.Open(src); err != nil {
		return err
	}
	defer srcfd.Close()

	if dstfd, err = os.Create(dst); err != nil {
		return err
	}
	defer dstfd.Close()

	if _, err = io.Copy(dstfd, srcfd); err != nil {
		return err
	}
	if srcinfo, err = os.Stat(src); err != nil {
		return err
	}
	return os.Chmod(dst, srcinfo.Mode())
}
