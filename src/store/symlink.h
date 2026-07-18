// Soft link resolution — transparent path→target following.
#pragma once
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace kvspace::store {

class SymlinkTable {
    static constexpr int MAX_HOPS = 16;

public:
    // 创建软链接
    void link(std::string_view target, std::string_view linkpath) {
        std::unique_lock lk(mu_);
        links_[std::string(linkpath)] = std::string(target);
    }

    // 删除链接（不影响 target）
    bool unlink(std::string_view linkpath) {
        std::unique_lock lk(mu_);
        return links_.erase(std::string(linkpath)) > 0;
    }

    // 解析路径：透明跟随所有软链接，最多 MAX_HOPS 跳
    // 返回最终 target 路径。如果路径本身不是链接，返回原路径。
    std::string resolve(std::string_view path) const {
        std::string cur(path);
        bool resolved = false;
        for (int hops = 0; hops < MAX_HOPS; hops++) {
            std::string target = lookup(cur);
            if (target.empty()) {
                resolved = true;
                break;
            }
            cur = target;
        }
        // 最后还有一层的话，顺着 target 拼接剩余部分
        // 例如: /alias → /real, 访问 /alias/x → /real/x
        return cur;
    }

    // resolve_prefix: 如果路径以某链接开头，替换前缀为 target
    // e.g. /alias → /real, 输入 /alias/x → /real/x
    std::string resolve_prefix(std::string_view path) const {
        std::shared_lock lk(mu_);
        std::string best_target;
        size_t best_len = 0;

        for (const auto& [link, target] : links_) {
            if (path.size() >= link.size() &&
                path.compare(0, link.size(), link) == 0 &&
                link.size() > best_len) {
                // match: path starts with link
                // also check: either exact match, or link ends with /, or next char is /
                if (path.size() == link.size() ||
                    link.back() == '/' ||
                    path[link.size()] == '/') {
                    best_target = target;
                    best_len = link.size();
                }
            }
        }

        if (best_len == 0) return std::string(path);

        std::string resolved = best_target;
        if (path.size() > best_len)
            resolved += std::string(path.substr(best_len));
        return resolved;
    }

    // 检查 path 是否是链接（精确匹配），若是返回 target
    std::string lookup(std::string_view path) const {
        std::shared_lock lk(mu_);
        auto it = links_.find(std::string(path));
        if (it != links_.end()) return it->second;
        return {};
    }

    // 判断删除操作的目标：若 key 本身是链接，只删链接；否则删数据
    bool is_link(std::string_view path) const {
        std::shared_lock lk(mu_);
        return links_.count(std::string(path)) > 0;
    }

    size_t count() const {
        std::shared_lock lk(mu_);
        return links_.size();
    }

private:
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::string> links_;  // linkpath → target
};

} // namespace kvspace::store
