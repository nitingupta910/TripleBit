local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-program,buildTripleBitFromN3,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
