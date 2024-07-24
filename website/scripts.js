    function loadTxtFile() {
        const txtSelect = document.getElementById('txtSelect');
        const txtContent = document.getElementById('txtContent');
        const fileName = txtSelect.value;

        //const baseFolderPath = '"D:/Course/大数据处理综合实验/Project/website/"'; // 基础文件夹路径
        let fullPath;

        if (fileName) {
            
            fullPath = fileName; 
            fetch(fullPath)
                .then(response => {
                    if (!response.ok) {
                        throw new Error('File not found');
                    }
                    return response.text();
                })
                .then(text => {
                    txtContent.textContent = text;
                })
                .catch(error => {
                    txtContent.textContent = '无法加载文件内容: ' + error.message;
                    console.error('Error loading file:', error);
                });
        } else {
            txtContent.textContent = '';
        }
}


function loadPngFile() {
    const pngSelect = document.getElementById('pngSelect');
    const pngImage = document.getElementById('pngImage');
    const fileName = pngSelect.value;

    if (fileName) {
        pngImage.src = fileName;
        pngImage.alt = fileName;
    } else {
        pngImage.src = '';
        pngImage.alt = 'PNG 文件';
    }
}
