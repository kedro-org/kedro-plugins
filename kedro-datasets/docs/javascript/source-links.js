// Make source code paths clickable and add [source] buttons
document.addEventListener('DOMContentLoaded', function() {
    const SOURCE_BUTTON_TIMEOUT_MS = 100;

    // Function to create a [source] button
    function createSourceButton(githubUrl) {
        const sourceButton = document.createElement('a');
        sourceButton.href = githubUrl;
        sourceButton.target = '_blank';
        sourceButton.rel = 'noopener noreferrer';
        sourceButton.textContent = '[source]';
        sourceButton.className = 'source-button';
        return sourceButton;
    }

    // Function to extract file path from source path text
    function extractFilePath(text) {
        // Look for patterns like "Source code in kedro_datasets/..." or direct file paths
        const sourceMatch = text.match(/Source code in ([^\s]+\.py)/);
        if (sourceMatch) {
            return sourceMatch[1];
        }
        // Check if it's already a file path
        if (text.includes('kedro_datasets') && text.endsWith('.py')) {
            return text.trim();
        }
        return null;
    }

    // Add [source] buttons to class signatures
    function addSourceButtonsToSignatures() {
        // Try multiple selectors to find the signature elements
        const possibleSelectors = [
            '.doc-signature',
            '.signature',
            'h1',
            'h2',
            'h3',
            'h4',
            'h5',
            'h6',
            '[data-md-component="content"] h1',
            '[data-md-component="content"] h2',
            '.md-typeset h1',
            '.md-typeset h2'
        ];

        let signatures = [];
        possibleSelectors.forEach(selector => {
            const elements = document.querySelectorAll(selector);
            signatures = signatures.concat(Array.from(elements));
        });

        signatures.forEach(function(signature) {
            // Skip if already has a source button
            if (signature.querySelector('a[href*="github.com"]')) {
                return;
            }

            const text = signature.textContent || signature.innerText;

            // Look for class names that match our datasets - more flexible regex
            const classMatch = text.match(/(kedro_datasets(?:_experimental)?[\.\w]*)/);
            if (classMatch) {

                // Find the corresponding source path in the document
                const allElements = document.querySelectorAll('*');
                let sourceFilePath = null;

                for (let element of allElements) {
                    const elementText = element.textContent || element.innerText;
                    if (elementText.includes('Source code in') && elementText.includes('.py')) {
                        sourceFilePath = extractFilePath(elementText);
                        break;
                    }
                }

                if (sourceFilePath) {
                    const githubUrl = `https://github.com/kedro-org/kedro-plugins/blob/main/kedro-datasets/${sourceFilePath}`;
                    const sourceButton = createSourceButton(githubUrl);

                    // Add the button to the signature
                    signature.appendChild(sourceButton);
                }
            }
        });
    }

    // Make existing source paths clickable
    function makeSourcePathsClickable() {
        const sourceElements = document.querySelectorAll('.doc-source-path, .doc-source, [class*="source"]');

        sourceElements.forEach(function(element) {
            const text = element.textContent || element.innerText;
            const filePath = extractFilePath(text);

            if (filePath) {
                const githubUrl = `https://github.com/kedro-org/kedro-plugins/blob/main/kedro-datasets/${filePath}`;

                // Create a clickable link
                const link = document.createElement('a');
                link.href = githubUrl;
                link.target = '_blank';
                link.rel = 'noopener noreferrer';
                link.textContent = text;
                link.className = 'source-link';

                // Replace the original text with the link
                element.innerHTML = '';
                element.appendChild(link);
            }
        });
    }

    function runAllFunctions() {
        addSourceButtonsToSignatures();
        makeSourcePathsClickable();
    }

    setTimeout(runAllFunctions, SOURCE_BUTTON_TIMEOUT_MS)

    // Also run when content is dynamically loaded
    const observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.type === 'childList' && mutation.addedNodes.length > 0) {
                setTimeout(runAllFunctions, SOURCE_BUTTON_TIMEOUT_MS);
            }
        });
    });

    observer.observe(document.body, {
        childList: true,
        subtree: true
    });
});
