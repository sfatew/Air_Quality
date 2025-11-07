from collections import OrderedDict
import os

class Limit_download_log:
    def __init__(self, max_size=5000, log_path=None, log_limit_lines=100000):
        self.max_size = max_size
        self.data = OrderedDict()
        self.log_path = log_path
        self.log_limit_lines = log_limit_lines

        # Load existing recent items if needed (optional)
        if self.log_path and os.path.exists(self.log_path):
            with open(self.log_path, "r") as f:
                for line in f:
                    item = line.strip()
                    if item:
                        self.add(item, log=False)

    #TODO: fix add method
    def add(self, item, log=True):
        if item in self.data:
            return  # already seen
        
        # remove oldest if over limit
        if len(self.data) >= self.max_size:
            self.data.popitem(last=False)
        self.data[item] = None
        
        # write to log file
        if log and self.log_path:
            with open(self.log_path, "a") as f:
                f.write(item + "\n")
            self._trim_log_if_needed()

    def _trim_log_if_needed(self):
        """Keep only the last `log_limit_lines` lines in log."""
        if not self.log_path:
            return
        size = os.path.getsize(self.log_path)
        if size > 10 * 1024 * 1024:  # ~10 MB safety check
            with open(self.log_path, "r+") as f:
                lines = f.readlines()
                if len(lines) > self.log_limit_lines:
                    f.seek(0)
                    f.writelines(lines[-self.log_limit_lines:])
                    f.truncate()

    def __contains__(self, item):
        return item in self.data

    def __repr__(self):
        return f"LimitedSet(size={len(self.data)}, max={self.max_size})"
