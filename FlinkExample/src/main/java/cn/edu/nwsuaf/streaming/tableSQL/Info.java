package cn.edu.nwsuaf.streaming.tableSQL;

/**
 * @ClassName: Info
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/12 7:34 下午
 */

public class Info {
    public String userId;
    public String name;
    public String sex;
    public String age;
    public long createTime;
    public long updateTime;

    public Info() {
    }   //要带有这个无参构造

    public Info(String userId, String name, String sex, String age, long createTime, long updateTime) {
        this.userId = userId;
        this.name = name;
        this.sex = sex;
        this.age = age;
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "Info{" +
                "userId=" + userId +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", age=" + age +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}
