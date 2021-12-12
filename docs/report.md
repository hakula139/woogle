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
- `<idf>`：表示这个短语 $t$ 在所有文档 $D$ 中的**逆向文件频率** IDF (Inverse Document Frequency)，使用科学计数法表示，这里我们采用的算法是 $$\mathrm{IDF}(t, D) = \log_2{\frac{N}{\lvert \{d\in D : t\in d\} \rvert}}$$ 其中：
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
electricity	1.386294e+00 04.txt:2:9.420631e-04:5756;12566
emergency	1.386294e+00 04.txt:1:4.710316e-04:6828
ethnic	1.386294e+00 03.txt:2:2.844950e-03:749;2960
european	6.931472e-01 01.txt:1:1.937984e-03:3047|04.txt:4:1.884126e-03:2981;3190;3814;11169
fall	6.931472e-01 04.txt:1:4.710316e-04:2408|01.txt:1:1.937984e-03:413
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

- `<tfidf_i>`：表示这个短语 $t$ 在文档 $d_i$ 中的 TF-IDF，使用科学计数法表示，这里我们采用的算法是 $$\mathrm{tfidf}(t, d, D) = \mathrm{tf}(t, d) \cdot \mathrm{idf}(t, D)$$ 通常，这个值可以作为这个文档在搜索结果中的权重。

例如：

```text
> back
back: IDF = 6.931472e-01 | found in files:
  02.txt: TF = 9.689922e-04 (1 times) | TF-IDF = 6.716542e-04 | positions: 3836
  03.txt: TF = 2.844950e-03 (2 times) | TF-IDF = 1.971969e-03 | positions: 518 1398
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
    - `io/`：一些自定义 Writable 类的定义，令 MapReduce 的 key 和 value 可以使用自定义类型。在使接口和实现更清晰可读、易于维护的同时，也节省了每次 `join` 成 String 再 `split` 回来的性能开销。
  - `xyz/hakula/woogle/`：package `xyz.hakula.woogle`，倒排索引检索功能的实现

### 5. 架构以及模块实现方法说明

## 贡献者

- [**Hakula Chen**](https://github.com/hakula139)<[i@hakula.xyz](mailto:i@hakula.xyz)> - 复旦大学

## 许可协议

本项目遵循 MIT 许可协议，详情参见 [LICENSE](../LICENSE) 文件。