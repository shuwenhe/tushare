# --- 编译器设置 ---
CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra

# --- 目标文件名 (修改为你想要的程序名) ---
TARGET   := tushare

# --- 库文件配置 ---
LIBS     := -lclickhouse-cpp-lib \
            -lcurl \
            -lzstd \
            -llz4 \
            -lcityhash \
            -lpthread

# --- 源文件与对象文件 (根据你的文件名修改为 tushare.cc) ---
SRCS     := tushare.cc
OBJS     := $(SRCS:.cc=.o)

# --- 编译规则 ---

all: $(TARGET)

$(TARGET): $(OBJS)
	@echo "正在链接生成可执行文件: $(TARGET)..."
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)
	@echo "构建成功！"

# 注意这里由于是 .cc 文件，规则要改为 %.o: %.cc
%.o: %.cc
	@echo "正在编译源文件: $<..."
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	@echo "正在清理生成的文件..."
	rm -f $(TARGET) $(OBJS)

.PHONY: all clean
