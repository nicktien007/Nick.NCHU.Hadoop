# Nick.NCHU.Hadoop

## PM25.java

路徑：`src/main/java/PM25.java`

- 使用 **K-Means**演算法實現分群，並使用**Hadoop**進行實作
- 使用2016 pm25資料集，資料集路徑=>`./pm25.txt`

### 演算法:

1. 從n個文件中隨機的選擇出k個文件作為質心
2. 從剩餘的文件中測量出每個文件到質心的距離,並歸類到最小質心的一類中
3. 重新計算質心的位置
4. 重複2-3步,直到迭代完成

### 操作說明：

> 上傳 pm25.txt資料集

```
hadoop fs -put pm25.txt
```

> 執行程式，參數說明：

`hadoop jar output.jar PM25 pm2.5文件路徑 分群數 迭代數 暫存檔路徑 結果路徑`

```
hadoop jar output.jar PM25 pm25.txt 4 30 t162 r162
```

### 參考資料
- https://bit.ly/3o9DZiH
- https://stanford.io/3596hkA
- https://bit.ly/31n2HSU
