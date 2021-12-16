# Woogle

本项目利用 Hadoop MapReduce，构建了对 Wikipedia 语料库的倒排索引，并实现了一个简易的搜索引擎，可根据检索的关键词返回相应的索引信息，使用 Java 编写。

## 目录

- [Woogle](#woogle)
  - [目录](#目录)
  - [项目报告](#项目报告)
    - [1. 任务说明与描述](#1-任务说明与描述)
    - [2. 参与人员任务分工说明](#2-参与人员任务分工说明)
    - [3. 程序启动与操作说明](#3-程序启动与操作说明)
      - [3.1 开发](#31-开发)
      - [3.2 启动](#32-启动)
      - [3.3 执行结果](#33-执行结果)
        - [3.3.1 索引格式](#331-索引格式)
        - [3.3.2 搜索结果格式](#332-搜索结果格式)
    - [4. 程序文件 / 类功能说明](#4-程序文件--类功能说明)
    - [5. 架构以及模块实现方法说明](#5-架构以及模块实现方法说明)
      - [5.1 总览](#51-总览)
      - [5.2 Driver](#52-driver)
      - [5.3 Job 1 - token position](#53-job-1---token-position)
        - [5.3.1 Driver](#531-driver)
        - [5.3.2 Mapper](#532-mapper)
        - [5.3.3 Reducer](#533-reducer)
      - [5.4 Job 2 - token count](#54-job-2---token-count)
        - [5.4.1 Driver](#541-driver)
        - [5.4.2 Mapper](#542-mapper)
        - [5.4.3 Reducer](#543-reducer)
      - [5.5 Job 3 - inverted index](#55-job-3---inverted-index)
        - [5.5.1 Driver](#551-driver)
        - [5.5.2 Mapper](#552-mapper)
        - [5.5.3 Reducer](#553-reducer)
      - [5.6 Woogle](#56-woogle)
        - [5.6.1 getPartition()](#561-getpartition)
        - [5.6.2 search()](#562-search)
        - [5.6.3 printResult()](#563-printresult)
  - [贡献者](#贡献者)
  - [许可协议](#许可协议)

## 项目报告

### 1. 任务说明与描述

> [Hadoop 平台使用及 PJ 要求](https://docs.qq.com/doc/DUnJVS0R6dURQU0lB)

在服务器上的 `/corpus/wiki` 目录下有 `0, 1, ..., 63.txt` 等 64 个文本文件，每个文件大小约为 300 MB，其内容格式为分行、无标点的英文文本，示例如下：

```text {.line-numbers}
lorem ipsum dolor sit amet consectetur adipisicing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua Ut enim ad minim veniam quis nostrud exercitation 
ullamco laboris nisi ut aliquip ex ea commodo consequat Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore 
eu fugiat nulla pariatur Excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum 
...
```

使用这些语料数据，计算文档中每个词的 TF-IDF（每个文件视为一个文档），要求实现以下功能：

1. 为每个出现的词构建索引，包括所属文档、出现次数、TF、IDF 信息；
2. 在上一步的基础上，包括此词在文档中出现的位置；
3. 支持关键词检索。实现程序，输入词后，程序输出这个词的索引。

### 2. 参与人员任务分工说明

- [**陈泓宜（18307130003）**](https://github.com/hakula139)：独立完成全部功能，实现了对语料库倒排索引的构建，实现了基于索引的关键词搜索功能。

### 3. 程序启动与操作说明

#### 3.1 开发

项目使用 IntelliJ IDEA 开发，相关构建、运行、打包配置已经写在了 `.idea` 目录下的配置文件里，直接在 IDE 里执行相应的任务即可：

- `index`：构建 package `xyz.hakula.index`，并执行其主类 `xyz.hakula.index.Driver` 的 `main()` 函数，传入参数 `input`, `output`, `temp`。这个包的功能是构建目录 `input` 下所有文件的倒排索引，索引结果保存在目录 `output` 下，执行过程中产生的临时数据放置在目录 `temp` 下。
- `woogle`：构建 package `xyz.hakula.woogle`，并执行其主类 `xyz.hakula.woogle.Woogle` 的 `main()` 函数，传入参数 `output`。这个包的功能是根据用户输入的关键词，在目录 `output` 下的索引里进行检索，最后输出这个关键词的倒排索引。
- `index_jar`：将上述 package `xyz.hakula.index` 打包为 JAR 包 `index.jar`，保存在目录 `jar` 下，之后同任务 `index` 一样执行
- `woogle_jar`：将上述 package `xyz.hakula.woogle` 打包为 JAR 包 `woogle.jar`，保存在目录 `jar` 下，之后同任务 `woogle` 一样执行

当然也可以通过 IDE 的构建选项只构建不执行，这里不作赘述。

原项目基于 Java SE 17 开发，为了兼容服务器的 Java SE 8 环境，在主分支 `master` 外维护了一个 `dev-jdk-1.8` 分支，提供了基于 Java 8 版本的实现，同时提供了配套的 IDE 配置文件。

#### 3.2 启动

项目已经预先打包好了 `index.jar` 和 `woogle.jar` 文件，可以直接使用。

对于 `index.jar` 文件，如果希望在本机上使用，则执行以下命令（需提前配置好 Java 环境）：

```bash
java -jar index.jar <input_path> <output_path> <temp_path>
```

其中，`<input_path>`, `<output_path>`, `<temp_path>` 分别表示指定的输入路径（语料库位置）、输出路径（索引位置）和缓存路径（临时文件位置）。需要注意的是，如果 `<output_path>`、`<temp_path>/output_job1` 和 `<temp_path>/output_job2` 中的某些在程序运行前已经存在，则程序会跳过部分任务的执行（具体跳过了什么、为什么跳过将在之后展开阐述）。因此如果你需要重新执行全部任务，则需要显式地将这些文件夹手动删除。

如果希望在 Hadoop 集群上使用，则执行以下命令（需提前配置好 Hadoop 环境）：

```bash
hadoop jar index.jar <input_path> <output_path> <temp_path>
```

对于 `woogle.jar` 文件，类似地执行以下命令：

```bash
java -jar woogle.jar <index_path>
```

其中，`<index_path>` 表示指定的索引位置，通常也就是前面传入 `index.jar` 的 `<output_path>`。

#### 3.3 执行结果

##### 3.3.1 索引格式

执行 `index.jar` 后，我们将在输出路径 `<output_path>` 下得到我们的索引文件，其内容格式如下：

```text {.line-numbers}
<token>	<idf> <filename_1>:<count_1>:<tf_1>:<position_1_1>;...;<position_1_c1>|...|<filename_n>:<count_n>:<tf_n>:<position_n_1>;...;<position_n_cn>
```

其中：

- `<token>`：表示一个短语 $t$
- `<idf>`：表示这个短语 $t$ 在所有文档 $D$ 中的**逆向文件频率** IDF (Inverse Document Frequency)，这里我们采用的算法是 $$\mathrm{IDF}(t, D) = \log_2{\frac{N}{\lvert \{d\in D : t\in d\} \rvert}}$$ 其中：
  - $N$：表示语料库中文档的总数 $\lvert D \rvert$
  - $\lvert \{d\in D : t\in d\} \rvert$：表示出现短语 $t$ 的文档总数
- `<filename_i>`：表示出现这个短语 $t$ 的第 $i$ 个文档 $d_i$ 的文件名
- `<count_i>`：表示这个短语 $t$ 在文档 $d_i$ 中出现的次数 $c_{t, d_i}$
- `<tf_i>`：表示这个短语 $t$ 在文档 $d_i$ 中的**词频** TF (Term Frequency)，使用科学计数法表示，这里我们采用的算法是 $$\mathrm{TF}(t, d) = \frac{c_{t, d}}{\sum_{t'\in d} c_{t', d}}$$ 其中：
  - $\sum_{t'\in d} c_{t', d}$：表示文档 $d$ 中的短语总数
- `<position_i_j>`：表示这个短语 $t$ 在文档 $d_i$ 中出现的位置 $p_{t, d_i, j}$，这里我们取的是该短语首字符关于文档起始位置的**字节偏移量**

- `<token>` 和 `<idf>` 之间以 `Tab` 分隔
- `<idf>` 和剩余部分之间以 `Space` 分隔
- `<filename>`, `<count>`, `<tf>`, `<positions>` 之间以 `:` 分隔
- `<position>` 之间以 `;` 分隔
- 不同文件对应的 `<filename>:<count>:<tf>:<positions>` 之间以 `|` 分隔

例如（这是我自己找的小测试样例的索引结果）：

```text {.line-numbers}
electricity	2.000000 04.txt:2:9.420631e-04:5756;12566
emergency	2.000000 04.txt:1:4.710316e-04:6828
ethnic	2.000000 03.txt:2:2.844950e-03:749;2960
european	1.000000 01.txt:1:1.937984e-03:3047|04.txt:4:1.884126e-03:2981;3190;3814;11169
fall	1.000000 04.txt:1:4.710316e-04:2408|01.txt:1:1.937984e-03:413
```

索引过程中产生的日志文件会保存在 `logs/app.log` 文件里（会随日期滚动）。

##### 3.3.2 搜索结果格式

执行 `woogle.jar` 后，程序会提示用户输入一个关键词：

```text
Please input a keyword:
>
```

输入关键词并回车后，程序将输出这个关键词的搜索结果，其格式如下：

```text
<token>: IDF = <idf> | found in files:
  <filename_1>: TF = <tf_1> (<count_1> times) | TF-IDF = <tfidf_1> | positions: <position_1_1> <position_1_2> ... <position_1_c1>
  <filename_2>: TF = <tf_2> (<count_2> times) | TF-IDF = <tfidf_2> | positions: <position_2_1> <position_2_2> ... <position_2_c2>
  ...
```

其中：

- `<tfidf_i>`：表示这个短语 $t$ 在文档 $d_i$ 中的 TF-IDF，使用科学计数法表示，这里我们采用的算法是 $$\mathrm{TFIDF}(t, d, D) = \mathrm{TF}(t, d) \cdot \mathrm{IDF}(t, D)$$ 通常，这个值可以作为这个文档在搜索结果中的权重。

例如：

```text
> back
back: IDF = 1.000000 | found in files:
  02.txt: TF = 9.689922e-04 (1 times) | TF-IDF = 9.689922e-04 | positions: 3836
  03.txt: TF = 2.844950e-03 (2 times) | TF-IDF = 2.844950e-03 | positions: 518 1398
```

这里其实我试着在不影响性能的前提下，稍微做了一点模糊检索，因此搜索结果中有时会不止出现这一个关键词，也会出现一些（但不是全部）包含这个关键词的短语。实际在命令行里展示时，短语里包含关键词的部分会标红加粗，这个是利用了 ANSI 字符颜色转义序列。

如果没有找到，程序则会输出：

```text
> aaaa
aaaa: not found
```

### 4. 程序文件 / 类功能说明

这里重点讲项目的核心代码部分，一些诸如 `log4j.properties` 之类的配置文件就略过了。

- `src/main/java/`：项目源代码
  - `xyz/hakula/index/`：package `xyz.hakula.index`，倒排索引构建功能的实现
    - `io/`：一些自定义 Writable 类型的定义，令 MapReduce 的 key 和 value 可以使用自定义类型。在使接口和实现更清晰可读、易于维护的同时，也节省了每次 `join` 成 `String` 再 `split` 回来的性能开销。因为比较 trivial，这里就不细讲了，可以直接看源代码，写得很清楚。
    - `Driver.java`：索引程序的主类，配置了所有的 Job，然后依次执行
    - `TokenPosition.java`：第 1 个 Job，读取目录 `<input_path>` 里的文件，提取所有短语在各文件中出现的位置，保存在路径 `<temp_path>/output_job1` 下
    - `TokenCount.java`：第 2 个 Job，读取目录 `<temp_path>/output_job1` 里的文件，统计所有短语在各文件中出现的次数，保存在路径 `<temp_path>/output_job2` 下；同时统计各文件的短语总数，保存在路径 `<temp_path>/file_token_count` 下对应的文件名里
    - `InvertedIndex.java`：第 3 个 Job，从路径 `<temp_path>/file_token_count` 里按需读取各文件的短语总数到内存中；然后读取目录 `<temp_path>/output_job2` 里的文件，计算所有短语在各文件中的 TF 以及其 IDF，保存在路径 `<output_path>` 下
  - `xyz/hakula/woogle/`：package `xyz.hakula.woogle`，倒排索引检索功能的实现
    - `model/`：一些自定义类型的定义，类似于 package `xyz.hakula.index` 下 `io/` 里的类，此外也提供了一些格式化输出索引的方法
    - `Woogle.java`：检索程序的主类，从终端读取用户输入，定位到对应的索引文件进行查询，然后利用 `model/` 里提供的方法格式化输出到终端

### 5. 架构以及模块实现方法说明

#### 5.1 总览

项目的整体架构分为 3 个 MapReduce Job。一般来说，关注程序的输入和输出是一个理清脉络的好方法。

开始时，输入数据的格式如下：

```text {.line-numbers}
<token> <token> <token> <token> <token> <token> <token> <token> <token> <token> <token> <token>
```

这里每个 `<token>` 就代表了一个短语。

首先这些数据经过 Job 1 - token position 的 Mapper，输出所有短语在各文件中出现的位置，格式如下：

```text {.line-numbers}
<token>@<filename>	<position>
```

其中，`Tab` 的左右侧分别是 key 和 value。这里每行的 `<position>` 只有 1 个。

然后这些数据经过 Job 1 的 Reducer，将同文件下相同短语（也就是 key 相同）的位置聚合了起来，输出所有短语在各文件中出现的位置数组，格式如下：

```text {.line-numbers}
<token>@<filename>	<position>;<position>;<position>
```

或者表示成：

```text {.line-numbers}
<token>@<filename>	[<position>]
```

至此 Job 1 结束，所有结果保存在目录 `<temp_path>/output_job1>` 下的文件里。为了节省 Job 间原始数据和 `String` 之间互相转换的开销，这里我们直接顺序输出二进制格式的数据（`SequenceFileOutputFormat`），因此直接打开文件是无法阅读的。

接下来这些数据经过 Job 2 - token count 的 Mapper，将 key 里的 `<token>` 字段移到 value 里，以便后续可以对 `<filename>` 聚合处理，格式如下：

```text {.line-numbers}
<filename>	<token>:[<position>]
```

然后这些数据经过 Job 2 的 Reducer，对于每个文件，统计各短语在其中出现的次数，并交换 `<token>` 和 `<filename>` 字段的位置，为 Job 3 做准备，格式如下：

```text {.line-numbers}
<token>	<filename>:<count>:0:[<position>]
```

这里这个 `0` 是 TF 的占位符，目前还无法计算（之后会讲为什么），因此先留空。

与此同时，我们对文件里所有短语的出现次数求和，从而得到文件的短语总数，格式如下：

```text {.line-numbers}
<total_count>
```

至此 Job 2 结束，所有结果保存在目录 `<temp_path>/output_job2` 下的文件里，每个文件的短语总数保存在文件 `<temp_path>/file_token_count/<filename>` 里。

接下来这些数据经过 Job 3 - inverted index 的 Mapper，根据文件 `<temp_path>/file_token_count/<filename>` 里保存的文件短语总数，计算得到 TF，替换掉原来的占位符，格式如下：

```text {.line-numbers}
<token>	<filename>:<count>:<tf>:[<position>]
```

最后这些数据经过 Job 3 的 Reducer，对于每个短语，将所有的 TF 信息聚合起来，得到的数组大小就是出现了这个短语的文件总数。然后根据预先在外侧（`xyz.hakula.index.Driver`）统计的文件总数，计算得到每个短语的 IDF，格式如下：

```text {.line-numbers}
<token>	<idf> <filename>:<count>:<tf>:[<position>]|<filename>:<count>:<tf>:[<position>]
```

或者表示成：

```text {.line-numbers}
<token>	<idf> [<filename>:<count>:<tf>:[<position>]]
```

这就是我们最后的输出结果，这个结果将以文本的形式保存。

下面我们来看看具体的实现。

#### 5.2 Driver

首先是索引程序的主类 `Driver`，也就是整个程序的入口。以下是基于 Java SE 17 的实现：

```java {.line-numbers}
// src/main/java/xyz/hakula/index/Driver.java

public class Driver extends Configured implements Tool {
  public static final int NUM_REDUCE_TASKS = 128;

  public static void main(String[] args) throws Exception {
    var conf = new Configuration();
    System.exit(ToolRunner.run(conf, new Driver(), args));
  }

  public int run(String[] args) throws Exception {
    var inputPath = new Path(args[0]);
    var outputPath = new Path(args[1]);
    var tempPath = new Path(args[2]);
    var tempPath1 = new Path(tempPath, "output_job1");
    var tempPath2 = new Path(tempPath, "output_job2");
    var fileTokenCountPath = new Path(tempPath, "file_token_count");

    var conf = getConf();
    try (var fs = FileSystem.get(conf)) {
      var totalFileCount = fs.getContentSummary(inputPath).getFileCount();
      if (totalFileCount == 0) return 0;
      conf.setLong("totalFileCount", totalFileCount);
      conf.set("fileTokenCountPath", fileTokenCountPath.toString());

      if (!fs.exists(tempPath1) && !runJob1(inputPath, tempPath1)) System.exit(1);
      if (!fs.exists(tempPath2) && !runJob2(tempPath1, tempPath2)) System.exit(1);
      if (!fs.exists(outputPath) && !runJob3(tempPath2, outputPath)) System.exit(1);
    }
    return 0;
  }
}
```

先看主体部分，开始时先从命令行参数里读取 `<input_path>`, `<output_path>`, `<temp_path>`，然后确定几个 Job 的输出路径 `<temp_path_1>`, `<temp_path_2>`, `<file_token_count_path>`。

接下来先利用 `fs.getContentSummary(inputPath).getFileCount()` 直接得到输入路径里的文件总数，为之后计算 IDF 做准备。为什么这样实现呢？因为这样比较简单，通过 MapReduce 会比较麻烦。

然后将这个文件总数 `totalFileCount` 和未来保存各文件短语总数的路径 `fileTokenCountPath` 写入配置 `conf`，以供接下来的 MapReduce Job 使用。

最后就是依次执行 3 个 Job 了，如果失败就退出。这里对路径是否存在做了一个判断，目的有两个：首先，如果输出路径已经存在的话，Hadoop 会抛异常。这是因为 Hadoop 在设计上是希望使用者一次写入、多次读取的，因此如果需要重新写入的话，需要显式地手动删除这个文件夹。其次，如果每次重启都直接删除所有文件夹的话，有点浪费，因为很多时候我们可能只希望重做其中一两个 Job（比如错误恢复的情形）。对于大数据集来说，保留一部分中间结果，重做时可以节省大量的时间。

接下来讲讲这 3 个 MapReduce Job。

#### 5.3 Job 1 - token position

##### 5.3.1 Driver

在 `Driver` 里，我们需要先对这个 Job 进行一些设定。

```java {.line-numbers}
// src/main/java/xyz/hakula/index/Driver.java

public class Driver extends Configured implements Tool {
  private boolean runJob1(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
    var job1 = Job.getInstance(getConf(), "token position");
    job1.setJarByClass(TokenPosition.class);

    job1.setMapperClass(TokenPosition.Map.class);
    job1.setMapOutputKeyClass(TokenFromFileWritable.class);
    job1.setMapOutputValueClass(LongWritable.class);

    job1.setReducerClass(TokenPosition.Reduce.class);
    job1.setNumReduceTasks(NUM_REDUCE_TASKS);
    job1.setOutputKeyClass(TokenFromFileWritable.class);
    job1.setOutputValueClass(LongArrayWritable.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, outputPath);

    return job1.waitForCompletion(true);
  }
}
```

主要做的事情是：

- 设定 Mapper 的类为 `TokenPosition.Map`，输出的 key 和 value 的类型分别为 `TokenFromFileWritable` 和 `LongWritable`
- 设定 Reducer 的类为 `TokenPosition.Reduce`，任务数量为 `128`，输出的 key 和 value 的类型分别为 `TokenFromFileWritable` 和 `LongArrayWritable`，输出到文件的格式为顺序输出二进制文件
- 设定输入路径为 `inputPath`，输出路径为 `outputPath`

最后等待 Job 1 完成。

那 Job 1 的核心类可想而知，就是 `TokenPosition` 了。我们来看看相关的实现。

##### 5.3.2 Mapper

```java {.line-numbers}
// src/main/java/xyz/hakula/index/TokenPosition.java

public class TokenPosition {
  public static class Map extends Mapper<LongWritable, Text, TokenFromFileWritable, LongWritable> {
    private final TokenFromFileWritable key = new TokenFromFileWritable();
    private final LongWritable offset = new LongWritable();

    // Yield the byte offset of a token in each file.
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      var filename = ((FileSplit) context.getInputSplit()).getPath().getName();
      var offset = key.get();  // byte offset

      var it = new StringTokenizer(value.toString(), " \t\r\f");
      while (it.hasMoreTokens()) {
        var token = it.nextToken().toLowerCase(Locale.ROOT);
        this.key.set(token, filename);
        this.offset.set(offset);
        context.write(this.key, this.offset);

        // Suppose all words are separated with a single whitespace character.
        offset += token.getBytes(StandardCharsets.UTF_8).length + 1;
      }
    }
  }
}
```

首先我们需要知道，最开始第一个 Mapper 直接从原始文件读取时，是按行读取的。也就是说，每个输入的 value 是源文件的一个行，而不是整个文件的内容。但接下来悲剧来了，key 是什么？很多教程里这个 key 的类型写的是 `Object`，因为他们并没有用到这个 key，实际上 key 的类型应该是 `LongWritable`。有些教程说 key 的含义是 value 在文件中的行号，这也是错的，实际上 key 代表的是 value 关于文档起始位置的**字节偏移量**，而且不是**列偏移量**。

为什么说这是个悲剧呢？因为这意味着想得到当前 value 实际在文件中的行号将变得异常困难，这是 Hadoop 的第一个坑。经过几天的研究，我目前了解到的有以下方案 [^1]：

1. 写个脚本程序对输入文件进行预处理，给每行的开头加一个行号
2. 重新实现一个 `InputFormat`，在最开始读入文件时，将 key 设定成行号

这两个方案我都不太满意，因此在进行了诸多尝试之后，我最后决定不做这个事了。因此目前用来表示短语在文档中位置的 `<position>` 的值，就是这个短语首字符关于文档起始位置的**字节偏移量**。其实这样在检索时也方便快速定位。

那么如何得到每个短语的字节偏移量 `<position>` 呢？考虑到语料库里所有短语都是用单个空格分隔的，那就好办多了。我们直接利用 `StringTokenizer` 将这行的内容分隔成一个个短语，然后每个短语的 `<position>` 就是前一个短语的 `<position>` 加上其所占字节数加 1，第一个短语的 `<position>` 就是行首关于文档起始位置的字节偏移量，也就是 key 的值。

最后我们设置输出的 key 为 `<token>@<filename>`，以便 Reducer 进一步聚合每个短语在各文件里出现的所有位置。输出的 value 就是短语出现的位置，也就是前面讲的字节偏移量。

##### 5.3.3 Reducer

```java {.line-numbers}
// src/main/java/xyz/hakula/index/TokenPosition.java

public class TokenPosition {
  public static class Reduce extends
      Reducer<TokenFromFileWritable, LongWritable, TokenFromFileWritable, LongArrayWritable> {
    private final LongArrayWritable offsets = new LongArrayWritable();

    // Yield all occurrences of a token in each file.
    @Override
    public void reduce(TokenFromFileWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      var offsets = new ArrayList<LongWritable>();
      for (var value : values) {
        offsets.add(WritableUtils.clone(value, context.getConfiguration()));
      }
      offsets.sort(LongWritable::compareTo);
      this.offsets.set(offsets.toArray(LongWritable[]::new));
      context.write(key, this.offsets);
    }
  }
}
```

Reducer 的逻辑就比较简单了，就是将每个短语在各文件里出现的所有位置排个序，然后聚合成一个数组，最后输出。这里使用 `SequenceFileOutputFormat` 的好处就体现出来了，我们可以直接输出自定义 Writable 类型，而不用一定要转化成 Text。

需要注意的是，MapReduce 在遍历一个 Iterable 时，为了节省内存开销，会**复用同一个 value 对象**，这是 Hadoop 的第二个坑。那我们知道 Java 底层全都是传的 reference，所以如果你直接将 value 传入数组的话，最后数组里所有元素的值就都会是同一个值（也就是最后一个元素）。因此这里传入数组的时候，一定要使用 `WritableUtils.clone()` 方法复制一个对象。

#### 5.4 Job 2 - token count

##### 5.4.1 Driver

```java {.line-numbers}
// src/main/java/xyz/hakula/index/Driver.java

public class Driver extends Configured implements Tool {
  private boolean runJob2(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
    var conf = getConf();
    var job2 = Job.getInstance(conf, "token count");
    job2.setJarByClass(TokenCount.class);

    job2.setInputFormatClass(SequenceFileInputFormat.class);
    job2.setMapperClass(TokenCount.Map.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(TokenPositionsWritable.class);

    var totalFileCount = conf.getLong("totalFileCount", 1);
    job2.setReducerClass(TokenCount.Reduce.class);
    job2.setNumReduceTasks((int) totalFileCount);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(TermFreqWritable.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, outputPath);

    return job2.waitForCompletion(true);
  }
}
```

和 Job 1 基本没什么区别，不再赘述。

这里将 Reducer 的任务数量设置为了文件总数，是因为这一步是在对 `<filename>` 进行聚合。这里最好是设置一个 Partitioner，让每个 `<filename>` 可以和 Reducer 一一对应，不过因为对效率影响不大，这里就不写了。

##### 5.4.2 Mapper

```java {.line-numbers}
// src/main/java/xyz/hakula/index/TokenCount.java

public class TokenCount {
  public static class Map
      extends Mapper<TokenFromFileWritable, LongArrayWritable, Text, TokenPositionsWritable> {
    private final Text key = new Text();
    private final TokenPositionsWritable value = new TokenPositionsWritable();

    // (<token>@<filename>, [<offset>]) -> (<filename>, (<token>, [<offset>]))
    @Override
    public void map(TokenFromFileWritable key, LongArrayWritable value, Context context)
        throws IOException, InterruptedException {
      this.key.set(key.getFilename());
      this.value.set(key.getToken(), (Writable[]) value.toArray());
      context.write(this.key, this.value);
    }
  }
}
```

Job 2 的 Mapper 就是字段改个位置，这个前面讲过了。接下来 Reducer 就可以对 `<filename>` 进行聚合。

##### 5.4.3 Reducer

```java {.line-numbers}
// src/main/java/xyz/hakula/index/TokenCount.java

public class TokenCount {
  public static class Reduce extends Reducer<Text, TokenPositionsWritable, Text, TermFreqWritable> {
    private final Text key = new Text();
    private final TermFreqWritable value = new TermFreqWritable();

    // Yield the token count of each token in each file,
    // and calculate the total token count of each file.
    // (<filename>, (<token>, [<offset>]))
    // -> (<token>, (<filename>, <token_count>, 0, [<positions>]))
    @Override
    public void reduce(Text key, Iterable<TokenPositionsWritable> values, Context context)
        throws IOException, InterruptedException {
      var filename = key.toString();
      long totalTokenCount = 0;
      for (var value : values) {
        var positions = value.getPositions();
        var tokenCount = positions.length;
        this.key.set(value.getToken());
        // The Term Frequency (TF) will be calculated in next job, and hence left blank here.
        this.value.set(filename, tokenCount, 0, positions);
        context.write(this.key, this.value);
        totalTokenCount += tokenCount;
      }
      writeToFile(context, key.toString(), totalTokenCount);
    }

    private void writeToFile(Context context, String key, long totalTokenCount) throws IOException {
      var conf = context.getConfiguration();
      var fs = FileSystem.get(conf);
      var fileTokenCountPath = conf.get("fileTokenCountPath");
      var outputPath = new Path(fileTokenCountPath, key);
      try (var writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)))) {
        writer.write(totalTokenCount + "\n");
      }
    }
  }
}
```

Reducer 比较复杂，是整个项目最大的难点。困难的不是实现本身，而是选择这个解决方案的思考过程。

我们的目标是得到 TF，现在我们有所有短语在各文件里出现的所有位置，因此我们就有所有短语在各文件里的出现次数。我们知道 $$\mathrm{TF}(t, d) = \frac{c_{t, d}}{\sum_{t'\in d} c_{t', d}}$$ 所以我们现在只需要各文件的短语总数，也就是将每个短语的出现次数求和。

听起来是不是很简单？我们已经有所有短语的出现次数了，直接遍历一下加起来不就行了？然而问题来了，现在我们不仅需要累加，而且还需要计算每个短语在各文件里的 TF。但问题是我们得先遍历一次，得到文件的短语总数，然后才能算出短语在这个文件里的 TF。

这有什么难的，那就先遍历一次求和，然后再遍历一次分别计算出 TF 的值不就好了？麻烦来了，MapReduce 为了节省内存开销，Iterable **只能遍历一次**，阅后即焚，这是 Hadoop 的第三个坑。

怎么解决呢？很简单，我直接开个数组把这些 value 都存下来不就好了。悲剧来了，`java.lang.OutOfMemoryError`！数据集太大，爆内存了。

看来遍历的时候不能将数据保存在内存里，必须直接写入文件。这下没办法在 Job 2 直接得到 TF 了，只能先用 0 占个位，我们到 Job 3 再算。问题又来了，那这些文件短语总数存在哪里呢？

一个很直观的想法是，那我在内存里建一个 HashMap，执行 Job 2 的过程中保存在里面，执行 Job 3 时再读取不就好了。至于这个 HashMap 放哪里，无所谓，反正基本约等于全局变量（准确来说是 static 变量），放 `Driver` 类里和放 `InvertedIndex` 类里都一样。

我一开始也是这么实现的，而且在本地跑得很正常，一点问题都没有。结果一上 Hadoop 集群傻眼了，`java.lang.NullPointerException`！写进 HashMap 的键值对，Job 3 读不到。

怎么一回事呢？原来，Hadoop 集群上**每个任务都单开了一个 JVM** [^2]，对于其他语言的实现就是单开了一个进程，这是 Hadoop 的第四个坑。所以你在这个 JVM 里写进内存的数据，其他 JVM 当然读不到了。其实仔细想想，显然是这么个道理，毕竟分布式系统，怎么可能所有任务都跑在同一个进程上。但第一次接触分布式系统的话，难免容易用单机的思路想问题，然后就上当了。

那怎么办呢？一种思路，也就是我目前的实现，是在执行 Job 2 的过程中，直接将文件短语总数写进文件里，之后执行 Job 3 前再读取到内存中。需要注意的是，不可以先写进内存，最后统一写进文件里，因为这同样会遇到前面提到的 JVM 分离的问题，不同任务的内存都是分开的。此外，为了避免并发写的问题，这里我将不同文件的短语总数都写到了不同的文件里（以 `<filename>` 命名）。这里还需要注意的是，学校服务器的 HDFS 似乎不支持 append 写，因此你也做不到把他们都写进一个文件里。

这个问题的解决方案想了我至少两天，可以说是本项目最大的难点了，期间真的是踩了不少坑。

别的就没什么好说的了，代码很直观。这里我们输出的 key 又变回了 `<token>`，接下来我们将对 `<token>` 进行聚合。

#### 5.5 Job 3 - inverted index

##### 5.5.1 Driver

```java {.line-numbers}
// src/main/java/xyz/hakula/index/Driver.java

public class Driver extends Configured implements Tool {
  private boolean runJob3(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
    var job3 = Job.getInstance(getConf(), "inverted index");
    job3.setJarByClass(InvertedIndex.class);

    job3.setInputFormatClass(SequenceFileInputFormat.class);
    job3.setMapperClass(InvertedIndex.Map.class);
    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(TermFreqWritable.class);

    job3.setReducerClass(InvertedIndex.Reduce.class);
    job3.setNumReduceTasks(NUM_REDUCE_TASKS);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(InvertedIndexWritable.class);

    FileInputFormat.addInputPath(job3, inputPath);
    FileOutputFormat.setOutputPath(job3, outputPath);

    return job3.waitForCompletion(true);
  }
}
```

和 Job 1 基本没什么区别，不再赘述。

这里不再需要设置 `setOutputFormatClass` 了，我们直接以文本格式输出。

##### 5.5.2 Mapper

```java {.line-numbers}
// src/main/java/xyz/hakula/index/InvertedIndex.java

public class InvertedIndex {
  public static class Map extends Mapper<Text, TermFreqWritable, Text, TermFreqWritable> {
    // Yield the Term Frequency (TF) of each token in each file.
    @Override
    public void map(Text key, TermFreqWritable value, Context context)
        throws IOException, InterruptedException {
      var fileTokenCount = readFromFile(context, value.getFilename());
      value.setTermFreq((double) value.getTokenCount() / fileTokenCount);
      context.write(key, value);
    }

    private long readFromFile(Context context, String key) throws IOException {
      var conf = context.getConfiguration();
      var fs = FileSystem.get(conf);
      var fileTokenCountPath = conf.get("fileTokenCountPath");
      var inputPath = new Path(fileTokenCountPath, key);
      try (var reader = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
        return Long.parseLong(reader.readLine());
      }
    }
  }
}
```

既然在 Job 2 的 Reducer 里不能得到 TF，那我们就在 Job 3 的 Mapper 里得到。当 Job 3 的 Mapper 需要一个文件的短语总数时，就从 Job 2 输出的中间文件里读取。顺便一提，MapReduce 的 Job 之间是顺序执行的，但同一个 Job 的 Mapper 和 Reducer 是并行的，因此我们也不能让 Mapper 或 Combiner 计算文件短语总数，然后在 Reducer 里读取。

将每个短语的出现次数除以文件的短语总数，我们就得到了短语的 TF，这下可以替换掉原来的占位符了。

##### 5.5.3 Reducer

```java {.line-numbers}
// src/main/java/xyz/hakula/index/InvertedIndex.java

public class InvertedIndex {
  public static class Reduce extends Reducer<Text, TermFreqWritable, Text, InvertedIndexWritable> {
    private final InvertedIndexWritable value = new InvertedIndexWritable();

    // Combine the Term Frequencies (TFs) of each token,
    // and yield the Inverse Document Frequency (IDF).
    // (<token>, (<filename>, <token_count>, 0, [<positions>]))
    // -> (<token>, (<idf>, [(<filename>, <token_count>, <tf>, [<positions>])]))
    @Override
    public void reduce(Text key, Iterable<TermFreqWritable> values, Context context)
        throws IOException, InterruptedException {
      var conf = context.getConfiguration();

      var termFreqList = new ArrayList<TermFreqWritable>();
      long fileCount = 0;
      for (var value : values) {
        termFreqList.add(WritableUtils.clone(value, conf));
        ++fileCount;
      }

      var totalFileCount = conf.getLong("totalFileCount", 1);
      var inverseDocumentFreq = Math.log((double) totalFileCount / fileCount) / Math.log(2);
      this.value.set(inverseDocumentFreq, termFreqList.toArray(TermFreqWritable[]::new));
      context.write(key, this.value);
    }
  }
}
```

最后的 Reducer 就是计算一下每个短语的 IDF。通过这次聚合，我们可以得到出现短语 key 的所有文档的 TF，遍历一次就可以得到出现这个短语的文档总数了。然后我们从配置 `conf` 里读取文档总数 `totalFileCount`，除一下取个对数就得到 IDF 了。最后以 `<token>` 为 key、其余数据为 value 全部写进文件，我们语料库的倒排索引就建立完成了。

#### 5.6 Woogle

下面简单讲讲检索程序的主类 `Woogle`，以下是基于 Java SE 17 的实现：

```java {.line-numbers}
// src/main/java/xyz/hakula/woogle/Woogle.java

public class Woogle extends Configured implements Tool {
  private static final String ANSI_RED = "\033[1;31m";
  private static final String ANSI_RESET = "\033[0m";

  private static final Logger log = Logger.getLogger(Woogle.class.getName());

  public static void main(String[] args) throws Exception {
    var conf = new Configuration();
    System.exit(ToolRunner.run(conf, new Woogle(), args));
  }

  public int run(String[] args) throws Exception {
    var key = "";
    try (var scanner = new Scanner(System.in)) {
      System.out.print("Please input a keyword:\n> ");
      key = scanner.nextLine().trim().toLowerCase(Locale.ROOT);
    }
    if (key.isBlank()) return 0;

    var partition = getPartition(key);
    var inputPath = new Path(args[0]);
    var filePath = new Path(inputPath, String.format("part-r-%05d", partition));

    var conf = getConf();
    var fs = FileSystem.get(conf);
    try (var reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
      search(reader, key);
    }
    return 0;
  }
}
```

先讲框架，流程上就是：

1. 提示用户输入一个关键词
2. 利用函数 `getPartition()` 得到关键词所在的索引文件位置
3. 遍历这个索引文件，搜索并输出相应的倒排索引

接下来讲一下具体实现。

##### 5.6.1 getPartition()

##### 5.6.2 search()

##### 5.6.3 printResult()

## 贡献者

- [**Hakula Chen**](https://github.com/hakula139)<[i@hakula.xyz](mailto:i@hakula.xyz)> - 复旦大学

## 许可协议

本项目遵循 MIT 许可协议，详情参见 [LICENSE](../LICENSE) 文件。

[^1]: [java - Get unique line number from a input file in MapReduce mapper - Stack Overflow](https://stackoverflow.com/questions/29786397/get-unique-line-number-from-a-input-file-in-mapreduce-mapper)  
[^2]: [java - Static variable value is not changing in mapper function - Stack Overflow](https://stackoverflow.com/questions/41280397/static-variable-value-is-not-changing-in-mapper-function)  
