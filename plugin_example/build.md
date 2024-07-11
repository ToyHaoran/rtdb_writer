# 插件编译为动态链接库
```shell
#rm -rf gowrite_plugin.*
go build -buildmode=c-shared -o gowrite_plugin.so
```