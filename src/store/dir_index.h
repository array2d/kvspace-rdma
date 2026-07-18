// Directory index — maintains prefix → child names for List support.
#pragma once
#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace kvspace::store {

class DirIndex {
public:
    // Set 时注册 key，从路径提取父目录并加入子项
    void add(std::string_view key) {
        std::string parent = dirname(key);
        if (parent.empty()) return;
        std::string child = basename(key);
        if (child.empty()) return;

        std::unique_lock lk(mu_);
        auto& children = index_[parent];
        auto it = std::lower_bound(children.begin(), children.end(), child);
        if (it == children.end() || *it != child)
            children.insert(it, child);
    }

    // Del 时移除子项记录
    void remove(std::string_view key) {
        std::string parent = dirname(key);
        if (parent.empty()) return;
        std::string child = basename(key);
        if (child.empty()) return;

        std::unique_lock lk(mu_);
        auto it = index_.find(parent);
        if (it == index_.end()) return;
        auto& children = it->second;
        auto ci = std::lower_bound(children.begin(), children.end(), child);
        if (ci != children.end() && *ci == child)
            children.erase(ci);
        if (children.empty()) index_.erase(it);
    }

    // 列出 prefix 的直接子项（不递归）
    std::vector<std::string> list(std::string_view prefix) const {
        std::shared_lock lk(mu_);
        std::string p(prefix);
        auto it = index_.find(p);
        if (it == index_.end()) return {};
        return it->second;
    }

    // 递归收集所有以 prefix 为前缀的 key（用于 DelTree）
    void collect_prefix(std::string_view prefix, std::vector<std::string>& out) const {
        std::shared_lock lk(mu_);
        std::string p(prefix);
        // match: dir == prefix OR dir starts with prefix/
        for (const auto& [dir, children] : index_) {
            bool match = (dir == p) ||
                (dir.size() > p.size() && dir[p.size()] == '/' &&
                 dir.compare(0, p.size(), p) == 0);
            if (match) {
                for (const auto& c : children) {
                    if (dir == "/")
                        out.push_back("/" + c);
                    else
                        out.push_back(dir + "/" + c);
                }
            }
        }
    }

    size_t size() const {
        std::shared_lock lk(mu_);
        return index_.size();
    }

private:
    // "/a/b/c" → "/a/b"
    static std::string dirname(std::string_view path) {
        if (path.empty() || path == "/") return "";
        auto pos = path.rfind('/');
        if (pos == std::string_view::npos) return "";
        if (pos == 0) return "/";
        return std::string(path.substr(0, pos));
    }

    // "/a/b/c" → "c"
    static std::string basename(std::string_view path) {
        if (path.empty()) return "";
        auto pos = path.rfind('/');
        if (pos == std::string_view::npos) return std::string(path);
        return std::string(path.substr(pos + 1));
    }

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::vector<std::string>> index_;
};

} // namespace kvspace::store
