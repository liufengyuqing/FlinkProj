package cn.edu.nwsuaf.streaming;

/**
 * @ClassName: WordCount
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/9 10:19 下午
 */

public class WordCount {
    public String word;
    public long count;

    public WordCount() {
    }

    public WordCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
