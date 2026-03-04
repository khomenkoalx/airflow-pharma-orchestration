import ftplib

def get_current_files_on_ftp(ftp, current_path):
    """Рекурсивно обходит директорию и собирает файлы с MDTM."""
    all_files = {}

    try:
        items = []
        ftp.retrlines(f'LIST {current_path}', items.append)

        for line in items:
            parts = line.split()
            if len(parts) < 9:
                continue

            name = ' '.join(parts[8:])
            full_path = f"{current_path.rstrip('/')}/{name}" if current_path != '/' else f"/{name}"

            file_type = line[0]

            if file_type == 'd':
                # Рекурсивно получаем файлы из подкаталога и обновляем словарь
                sub_files = get_current_files_on_ftp(ftp, full_path)
                all_files.update(sub_files)
            elif file_type == '-':
                try:
                    mdtm_resp = ftp.sendcmd(f'MDTM {full_path}')
                    mdtm = mdtm_resp[4:] if mdtm_resp.startswith('213 ') else 'N/A'
                except ftplib.error_perm:
                    mdtm = 'N/A'
                all_files[full_path] = mdtm

    except ftplib.error_perm as e:
        print(f"Нет доступа к {current_path}: {e}")
        return {}

    return all_files


def 