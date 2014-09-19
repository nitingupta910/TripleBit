sub_dir :=  $(call source-dir-to-binary-dir, TripleBit/util)
$(shell $(MKDIR) $(sub_dir))

local_src := $(wildcard $(subdirectory)/*.cpp) $(wildcard $(subdirectory)/util/*.cpp)

$(eval $(call make-library,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
