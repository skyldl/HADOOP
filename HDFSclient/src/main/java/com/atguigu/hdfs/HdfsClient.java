package com.atguigu.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.kerby.config.Conf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码常用套路
 * 1.获取一个客户端对象
 * 2.执行相关的操作命令
 * 3.关闭资源
 * HDFS zookeeper
 */


public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration Configuration = new Configuration();

        // 设置副本数,客户端代码中设置的值优先级最高
        Configuration.set("dfs.replication", "2");
        String user = "atguigu";
        // 获取一个客户端对象,参数1:uri,参数2:配置文件,参数3:用户名   idea小技巧 ctrl+alt+f 抽取变量,直接回车变为全局变量
        fs = FileSystem.get(uri, Configuration,user);
    }
    @After
    public void close() throws IOException {
        // 关闭资源
        fs.close();
    }
    @Test
    public void textmkdir() throws URISyntaxException, IOException, InterruptedException {
        // 创建一个目录
        fs.mkdirs(new Path("/xiyou/huaguoshan"));

    }

    /**
     * 参数优先级
     * 1.客户端代码中设置的值 > classpath(resources)下的用户自定义配置文件 > 服务器的默认配置
     * hdfs-default.xml ==> hdfs-site.xml ==>
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        // 上传文件,参数1:是否删除源文件,参数2:是否允许覆盖,参数3:本地路径,参数4:目标路径
        fs.copyFromLocalFile(false,true,new Path("d:/input/wordcount.txt"),new Path("/xiyou/huaguoshan/wordcount.txt"));
    }

    // 文件下载
    @Test
    public void testGet() throws IOException {
        // 参数1:是否删除源文件,参数2:源路径HDFS,参数3:目标地址路径,参数4:是否校验文件
        fs.copyToLocalFile(false,new Path("/xiyou/huaguoshan/wordcount.txt"),new Path("d:/input/wordcount.txt"),true);
    }

    // 文件删除
    @Test
    public void testDelete() throws IOException {
        // 参数1:要删除的文件路径,参数2:是否递归删除
        fs.delete(new Path("/jdk-8u212-linux-x64.tar.gz"),true);

        // 删除空目录,参数1:要删除的目录路径,参数2:是否递归删除
        fs.delete(new Path("/xiyou"),false);

        // 删除非空目录,参数1:要删除的目录路径,参数2:是否递归删除
        fs.delete(new Path("/xiyou"),true);
    }

    // 文件的更名和移动
    @Test
    public void testRename() throws IOException {
        // 参数1:原文件路径,参数2:目标文件路径
        fs.rename(new Path("/xiyou/huaguoshan/wordcount.txt"),new Path("/xiyou/huaguoshan/wordcount1.txt"));
    }

    // 获取文件详情信息
    @Test
    public void testListFiles() throws IOException {
        // 参数1:要列举的目录路径,参数2:是否递归列举
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();


            System.out.println("=============" + status.getPath() + "============="); // 文件路径
            System.out.println("权限: " + status.getPermission()); // 权限
            System.out.println("所属用户: " + status.getOwner()); // 所属用户
            System.out.println("所属组: " + status.getGroup()); // 所属组
            System.out.println("文件长度: " + status.getLen()); // 文件长度
            System.out.println("修改时间: " + status.getModificationTime()); // 修改时间
            System.out.println("副本数: " + status.getReplication()); // 副本数
            System.out.println("块大小: " + status.getBlockSize()); // 块大小
            System.out.println("访问时间: " + status.getAccessTime()); // 访问时间
            System.out.println("文件名称: " + status.getPath().getName());// 文件名称

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            System.out.println("块信息: " + Arrays.toString(blockLocations));
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("存储块: " + host);
                }
            }
            System.out.println("-----------华丽的分割线-----------");
        }
    }

    // 判断是文件还是目录
    @Test
    public void testListStatus() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()) {
                System.out.println("文件: " + fileStatus.getPath().getName());
            } else {
                System.out.println("目录: " + fileStatus.getPath().getName());
            }
        }
    }
}
