/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbfconcat;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.application.Application;

/**
 *
 * @author spidchenko.d
 */

class Field{
    private String name;    //Название столбца
    private int size;       //Ширина столбца в байтах
    private int offset;     //Смещение от начала записи в байтах
        
    public Field(String newName, int newSize, int newOffset){
        name = newName;
        size = newSize;
        offset = newOffset;
    }
        
    static Field[] initializeFieldsArray(int fieldsCount){
        Field [] fieldArrayReturn = new Field [fieldsCount];  //Массив столбцов
        
        for (int i = 0; i< fieldArrayReturn.length; i++){
                fieldArrayReturn[i] = new Field("", 0, 0);
            }
        return fieldArrayReturn;
    }
//<editor-fold defaultstate="collapsed" desc="SETERS-GETERS">
    void setName(String newName){
        name = newName;
    }
    
    void setSize(int newSize){
        size = newSize;
    }
    
    void setOffset(int newOffset){
        offset = newOffset;
    }
    
    String getName(){
        return name;
    }
    
    int getSize(){
        return size;
    }
    int getOffset(){
        return offset;
    }
//</editor-fold>
}

class DbfFile {
    static final int SERVICE_HEADER_LENGTH = 32;//Первые 32 байта файла - служебная информация
    static final int FIELD_DESCRIPTION_LENGTH = 32;  //Длина описания столбца - 32 байта
    static final int CURRENT_FIELD_NAME = 9;    //0-9 байты
    static final int CURRENT_FIELD_LENGTH = 16; //16й байт с длиной текущего столбца
    static Charset fileCharset = null;//Charset.forName(new appSettings().fields.getDbfEncoding());//Charset.forName("cp866");//Кодировка
    private int headerLength = 0;                //Полная длина заголовка в байтах (из 8-9 байта)
    private int numOfFields = 0;                 //Количество столбцов таблицы
    private int numOfRecords = 0;                //Количество записей в таблице
    private int oneRecordLength = 0;             //Длина одной записи в байтах
    private Field [] fieldArray;
        //------
    //private String filePath = "";//E:\\0302.dbf"; 
    private Object[][] tableData;               //Данные для отображения в jTable
    private java.io.File file = null;           //Файл
    private FileInputStream inputStream = null; //InputStream
    
        
    public DbfFile(java.io.File fileToOpen, String charset){
        file = fileToOpen;
        //filePath = filePathToOpen.toString();
        fileCharset = Charset.forName(charset);
        
        //--
        
        byte[] byteBufferArray = new byte[1024];   //Байтовый буфер для чтения. Самая длинная запись которую я видел - 505 байт, пусть будет в 2 раза больше
                
        try{
            inputStream = new FileInputStream(file.toString());
            inputStream.read(byteBufferArray, 0, SERVICE_HEADER_LENGTH);
            //Проверка сигнатуры .dbf файла
            if(byteBufferArray[0] != 3) System.out.println("Bad Dbf File: "+getFile());
            //Делаем unsigned byte массив [4-7] байтов (количество записей, старший байт справа)
            int [] byteArray = new int[4];
            for(int i = 0; i<4; i++){
                byteArray[i] = (byteBufferArray[i+4]>0)?byteBufferArray[i+4]:(byteBufferArray[i+4] & 0xFF);
            }
            //сдвигаем байты (старший слева) и получаем количество записей (32бит число)
            numOfRecords = byteArray[0]|(byteArray[1]<<8)|(byteArray[2]<<16)|(byteArray[3]<<24);
            //Делаем unsigned byte массив [8-9] байтов (количество байт в заголовке, старший байт справа)
            for(int i = 0; i<2; i++){
                byteArray[i] = (byteBufferArray[i+8]>0)?byteBufferArray[i+8]:(byteBufferArray[i+8] & 0xFF);
            }
            //сдвигаем байты (старший слева) и получаем длину одной записи (16бит число)
            headerLength = byteArray[0]|(byteArray[1]<<8);
            //headerLength+=1;
            //Делаем unsigned byte массив [10-11] байтов (длина одной записи, старший байт справа)
            for(int i = 0; i<2; i++){
                byteArray[i] = (byteBufferArray[i+10]>0)?byteBufferArray[i+10]:(byteBufferArray[i+10] & 0xFF);
            }           
            //сдвигаем байты (старший слева) и получаем длину одной записи (16бит число)
            oneRecordLength = byteArray[0]|(byteArray[1]<<8);
        
            //Считаем количество столбцов в таблице
            numOfFields = (headerLength - SERVICE_HEADER_LENGTH - 1)/FIELD_DESCRIPTION_LENGTH;
            
            inputStream = new FileInputStream(file.toString());    //Откроем еше раз, чтобы вернуться в начало файла, как иначе хз
            inputStream.skip(SERVICE_HEADER_LENGTH);    //Пропустили служебный предзаголовок

        //Парсим описания столбцов таблицы (fieldArray):
            fieldArray = Field.initializeFieldsArray(numOfFields);  //Инициализируем массив столбцов
            for (int i = 0; i < fieldArray.length; i++){
                inputStream.read(byteBufferArray, 0, FIELD_DESCRIPTION_LENGTH);    //32 байта 
                //Название столбца (вытащили из байтового массива и убрали пробелы с конца, перевели в верхний регистр одной коммандой! >:3 )
                //new String корректно отработает с default charset ASCII, на линуксе или в Японии с UTF Default будут проблемы 
                fieldArray[i].setName(new String(Arrays.copyOfRange(byteBufferArray, 0, CURRENT_FIELD_NAME)).trim().toUpperCase());  //9 байт
                //Размер столбца
                if (byteBufferArray[CURRENT_FIELD_LENGTH] > 0){
                    fieldArray[i].setSize(byteBufferArray[CURRENT_FIELD_LENGTH]);
                } else{
                    fieldArray[i].setSize(byteBufferArray[CURRENT_FIELD_LENGTH] & 0xFF);
                }
                //Сдвиг от начала записи в байтах
                if (i != 0){
                    fieldArray[i].setOffset(fieldArray[i-1].getOffset()+fieldArray[i-1].getSize());
                } else{
                    fieldArray[i].setOffset(0);
                }
            }
        //---    

        //Парсим строки таблицы (tableData):
            String currentLine = "";
            //Файловый курсор сейчас перед 0xD [0xD, 0x0]
            inputStream = new FileInputStream(file.toString());  //Откроем еше раз, чтобы вернуться в начало файла, как иначе хз
            inputStream.skip(headerLength+1);       //Пропустили весь заголовок +1 байт
                       
            tableData = new String [numOfRecords][numOfFields];
            for (int i = 0; i < numOfRecords; i++){
                //Считали одну запись в byteBufferArray
                inputStream.read(byteBufferArray, 0, oneRecordLength);
                //Декодировали массив byteBufferArray, обернутый в байтбуффер в UTF-16 
                //Имеем на выходе строку UTF-16 с полями из DBF файла
                currentLine = fileCharset.decode(ByteBuffer.wrap(byteBufferArray,0, oneRecordLength)).toString();
                for(int j =0; j < numOfFields; j++){
                    tableData[i][j] = currentLine.substring(fieldArray[j].getOffset(),
                    fieldArray[j].getOffset() + fieldArray[j].getSize()).trim();
                }
            }
        //---    
        } catch (IOException ex) {
            Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (inputStream != null) {try {
                inputStream.close();
                } catch (IOException ex) {
                    Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
            //--
    }//End of DbfFile Constructor method
    
//<editor-fold defaultstate="collapsed" desc="GETERS-SETERS">
    int getNumOfRecords(){
        return numOfRecords;
    }
    
    java.io.File getFile(){
        return file;
    }
    
    int getNumOfFields(){
        return numOfFields;
    }
    
    Field[] getFieldArray(){
        return fieldArray;
    }
    /**
     * Формирует массив с названиями столбцов из массива объектов fieldArray
     */     
     String[] getTableTitles(){
        //Формируем для таблицы jTable данные
        //Извлекли из массива только названия столбцов для отображения:
        String tableTitles[] = new String[fieldArray.length];
        for(int i = 0; i < fieldArray.length; i++){
            tableTitles[i] = fieldArray[i].getName();
        }
        return tableTitles;
    }

    /**
    * Возвращает массив записей для jTable
    */     
    Object[][] getTableDataToShow(){    
        return tableData;
    }
   
 
    int getOneRecordLength(){
        return oneRecordLength;
    }
//</editor-fold>
    /**
    * Печать служебной информации, заголовков в консоль
    */
    void printFileInfo(){
        System.out.print("\n");
        System.out.format("headerLength: %4d \nnumOfFields:%3d \nnumOfRecords:%4d \noneRecordLength:%4d\n\n", headerLength, numOfFields, numOfRecords, oneRecordLength);
            
        for (Field fieldArray1 : fieldArray) {
            System.out.format("%10s | %5d | %5d \n", fieldArray1.getName(), fieldArray1.getSize(), fieldArray1.getOffset());
        }
        System.out.print("\n");        
    }
    /**
    * Печать содержимого файла в консоль
    */    
    void printRecords(){
        for(int i = 0; i < numOfRecords; i++){
            for(int j = 0; j < numOfFields; j++){
                System.out.printf("%"+fieldArray[j].getSize()+"s",getTableDataToShow()[i][j]);
            }            
        System.out.print("\n");
        }
    }
    /**
    * Возвращает байтовый массив свех записей без служебного заголовка
    */ 
    byte[] getAllRecords(){
        byte[] byteBufferArray = new byte[numOfRecords*oneRecordLength];
        try {
            inputStream = new FileInputStream(file.toString());
            inputStream.skip(headerLength+1);   //пропустили служебный заголовок +1 байт
            inputStream.read(byteBufferArray, 0, numOfRecords*oneRecordLength);
  //          System.out.println(new String(byteBufferArray));
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
        }
        return byteBufferArray;
    }
    /**
    * Добавим записи в конец файла
    */ 
    void append(DbfFile secondFile) throws IOException{
        
        //numOfRecords += secondFile.getNumOfRecords();
        int newNumOfRecords = numOfRecords + secondFile.getNumOfRecords();
        
        ByteBuffer bytesBuffer = ByteBuffer.allocate(4);
        bytesBuffer.order(ByteOrder.LITTLE_ENDIAN);
        bytesBuffer.putInt(newNumOfRecords);
        byte[] numIn4Bytes = bytesBuffer.array();
        
        FileOutputStream fileOutputStream; 
        try {
            int sizeOfFile = (int)file.length();  //SERVICE_HEADER_LENGTH + 2 +
            //    + numOfFields*FIELD_DESCRIPTION_LENGTH +
            //    + numOfRecords * oneRecordLength; 
            
            
            byte[] byteBufferArray = new byte[sizeOfFile];
            //System.out.println(sizeOfFile+" = "+(int)file.length());//!!!!!!!!!!!!!!!!!11111111111111
            inputStream = new FileInputStream(file.toString());
            
            inputStream.read(byteBufferArray, 0, sizeOfFile);
            inputStream.close();
            
            //Заполняем 4 байта новыми значениями
            for(int i=0; i<4; i++){
                byteBufferArray[i+4] = numIn4Bytes[i];
            }
            //последний финальный байт 0х1А заменяем на 0х20
            byteBufferArray[byteBufferArray.length - 1] = 0x20;
            
            fileOutputStream = new FileOutputStream(getFile());
            fileOutputStream.write(byteBufferArray);
            fileOutputStream.write(secondFile.getAllRecords());
            fileOutputStream.flush();
            fileOutputStream.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    void renumerate() throws IOException{
        try {
             byte[] byteBufferArray = new byte[(int)file.length()];
            FileOutputStream fileOutputStream;
            
            inputStream = new FileInputStream(file.toString());
            inputStream.read(byteBufferArray, 0, (int)file.length());
            inputStream.close();
            
            //Некоторая 16-ричная магия. Адрес байта с номером строки - 0x20C
            //повторения через (recordLength)0x101 байт
            int recordHexAdr = 0x20C;
            int recordNum = 1;
            while ( recordHexAdr < (int)file.length()){         //Необходимо вставлять цифру как байты в кодировке ANSI
                byteBufferArray[recordHexAdr-3] = recordNum < 1000? 0x20 : (byte)(recordNum /1000     +0x30);
                byteBufferArray[recordHexAdr-2] = recordNum < 100 ? 0x20 : (byte)(recordNum /100      +0x30);
                byteBufferArray[recordHexAdr-1] = recordNum < 10  ? 0x20 : (byte)(recordNum / 10 % 10 +0x30);
                byteBufferArray[recordHexAdr] = (byte)(recordNum % 10 +0x30);  
                recordHexAdr += getOneRecordLength();
                recordNum++;
            }
            
            fileOutputStream = new FileOutputStream(getFile());
            fileOutputStream.write(byteBufferArray);
            fileOutputStream.flush();
            fileOutputStream.close();
            
            
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DbfFile.class.getName()).log(Level.SEVERE, null, ex);
        }            
    }
    
    
}


/**
 *
 * @author spidchenko.d
 */
public class DbfConcat {

    public static void copyFile(String origin, String destination) throws IOException {
        Path FROM = Paths.get(origin);
        Path TO = Paths.get(destination);
        //overwrite the destination file if it exists, and copy
        // the file attributes, including the rwx permissions
        CopyOption[] options = new CopyOption[]{
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.COPY_ATTRIBUTES
        }; 
        Files.copy(FROM, TO, options);
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException{
        File folder = new File(System.getProperty("user.dir"));
        System.out.println(System.getProperty("user.dir"));
        File[] arrayOfFiles = folder.listFiles(new FilenameFilter(){
                                            @Override
                                            public boolean accept(File dir, String name){
                                                    return name.endsWith(".dbf");
        }});
        
        DbfFile firstDbf, secondDbf;
        try{
            copyFile(arrayOfFiles[0].getName(), "concat.dbf");            
        }catch(ArrayIndexOutOfBoundsException e){
            System.err.println("No dfb files found :/(");
            return;     //Exit with error
        }

        
        for(int i=0; i< arrayOfFiles.length-1; i++){
            System.out.println(arrayOfFiles[i].getName());
            secondDbf = new DbfFile(new File(arrayOfFiles[i+1].getName()), "windows-1251");
            new DbfFile(new File("concat.dbf"), "windows-1251").append(secondDbf);
        }
        
        new DbfFile(new File("concat.dbf"), "windows-1251").renumerate();
        
        //System.out.println(firstDbf.getOneRecordLength());
              
//            //Делаем unsigned byte массив [4-7] байтов (количество записей, старший байт справа)
//            int [] byteArray = new int[4];
//            for(int i = 0; i<4; i++){
//                byteArray[i] = (byteBufferArray[i+4]>0)?byteBufferArray[i+4]:(byteBufferArray[i+4] & 0xFF);
//            }
//            //сдвигаем байты (старший слева) и получаем количество записей (32бит число)
//            numOfRecords = byteArray[0]|(byteArray[1]<<8)|(byteArray[2]<<16)|(byteArray[3]<<24);       
//        byte [] rawByteArray = {(byte)0xE9,(byte)0x01,(byte)0x00,(byte)0x00};
//        //Используем байтбуффер для переворота байтов в BIG_ENDIAN формат
//        ByteBuffer bb = ByteBuffer.allocate(4);// = new ByteBuffer();//ByteBuffer.wrap(rawByteArray);
//        bb.order(ByteOrder.LITTLE_ENDIAN);
//        bb.putInt(489);
//        System.out.println(Arrays.toString(bb.array()));
        
        //System.out.println(bb.getInt()+" L E");
        
//        int testReversBytes = (rawByteArray[0]<<24)|(rawByteArray[1]<<16)|(rawByteArray[2]<<8)|rawByteArray[3];
//        int [] byteArray = new int[4];
//        for(int i = 0; i<4; i++){
//            byteArray[i] = (rawByteArray[i]>0)?rawByteArray[i]:(rawByteArray[i] & 0xFF);
//        }
//        int decNum = byteArray[0]|(byteArray[1]<<8)|(byteArray[2]<<16)|(byteArray[3]<<24);
//        System.out.println(decNum);
//        System.out.println(Integer.toHexString(testReversBytes)+"="+Integer.toBinaryString(testReversBytes));
//        int rev = Integer.reverseBytes(testReversBytes);
//        System.out.println("     "+rev+"=00000000000000000000000"+Integer.toBinaryString(rev));
        //System.out.println("         "+(Byte.toBinaryString((byte)(rev<<24>>>24))));
        //System.out.`
        
        
//            secondDbf = new DbfFile(new File("test2.dbf"), "windows-1251");
//            firstDbf.append(secondDbf);
//            firstDbf.printFileInfo();
//            firstDbf.printRecords();
        //    Charset fileCharset = Charset.forName("windows-1251");
       //     System.out.println(fileCharset.decode(ByteBuffer.wrap(inputDbf.getAllRecords())).toString());
        
        
    }
    
}
