# Third-Party Notices and Licenses

NornicDB is released under the MIT License. This file contains the notices and licenses for third-party software and models included or bundled with NornicDB.

---

## Table of Contents

1. [Bundled AI Models](#bundled-ai-models)
   - [BGE-M3 Embedding Model](#bge-m3-embedding-model)
   - [qwen3-0.6b-Instruct](#qwen25-05b-instruct)
2. [Go Dependencies](#go-dependencies)
3. [C/C++ Libraries](#cc-libraries)
4. [JavaScript/TypeScript Dependencies](#javascripttypescript-dependencies)

---

## Bundled AI Models

NornicDB's Heimdall-enabled Docker images include pre-trained AI models that are automatically downloaded during the build process. These models have their own licenses separate from NornicDB's MIT license.

### BGE-M3 Embedding Model

**Source:** https://huggingface.co/BAAI/bge-m3  
**Quantized Version:** https://huggingface.co/gpustack/bge-m3-GGUF  
**License:** MIT License  
**Copyright:** Beijing Academy of Artificial Intelligence (BAAI)

**Model Details:**
- File: `bge-m3-Q4_K_M.gguf`
- Purpose: Text embedding generation for semantic search
- Downloaded from: https://huggingface.co/gpustack/bge-m3-GGUF/resolve/main/bge-m3-Q4_K_M.gguf

**License Text:**
```
MIT License

Copyright (c) 2023 Beijing Academy of Artificial Intelligence (BAAI)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

### qwen3-0.6b-Instruct

**Source:** https://huggingface.co/Qwen/qwen3-0.6b-Instruct  
**Quantized Version:** https://huggingface.co/Qwen/qwen3-0.6b-Instruct-GGUF  
**License:** Apache License 2.0  
**Copyright:** Alibaba Cloud

**Model Details:**
- File: `qwen3-0.6b-instruct-q4_k_m.gguf`
- Purpose: Language model for Heimdall intelligent assistance
- Downloaded from: https://huggingface.co/Qwen/qwen3-0.6b-Instruct-GGUF/resolve/main/qwen3-0.6b-instruct-q4_k_m.gguf

**License Text:**
```
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS
```

**Citation:**
```
@article{qwen2.5,
  title={Qwen2.5: A Party of Foundation Models},
  url={https://qwenlm.github.io/blog/qwen2.5/},
  author={Qwen Team},
  year={2024}
}
```

---

## Go Dependencies

All Go dependencies are listed in `go.mod`. The following have licenses other than MIT:

### Apache License 2.0

- **github.com/dgraph-io/badger/v4** - Embedded key-value database
- **github.com/dgraph-io/ristretto/v2** - High-performance cache
- **github.com/spf13/cobra** - CLI framework
- **go.opentelemetry.io/** packages - OpenTelemetry instrumentation
- **google.golang.org/protobuf** - Protocol Buffers

### BSD-3-Clause License

- **github.com/google/uuid** - UUID generation
- **github.com/spf13/pflag** - POSIX/GNU-style flags
- **golang.org/x/crypto** - Go cryptography
- **golang.org/x/net** - Go networking
- **golang.org/x/sys** - Go system calls

### ISC License

- **github.com/davecgh/go-spew** - Deep pretty printing
- **gopkg.in/yaml.v3** - YAML parser

**Note:** All Go dependencies are compatible with NornicDB's MIT license and allow commercial use, modification, and distribution.

---

## C/C++ Libraries

### llama.cpp

**Source:** https://github.com/ggerganov/llama.cpp  
**License:** MIT License  
**Copyright:** Georgi Gerganov and contributors

**Description:** Inference engine for LLM models in GGUF format. NornicDB includes statically compiled versions for multiple platforms (macOS Metal, Linux CPU/CUDA, Windows CUDA).

**License Text:**
```
MIT License

Copyright (c) 2023-2024 The ggml authors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### GGML

**Source:** https://github.com/ggerganov/ggml (part of llama.cpp)  
**License:** MIT License  
**Copyright:** Georgi Gerganov

**Description:** Tensor library for machine learning, used by llama.cpp for model inference.

### musl libm

**Source:** https://musl.libc.org/  
**License:** MIT License  
**Copyright:** Copyright © 2005-2024 Rich Felker, et al.

**Description:** Double-precision math functions ported to Go in `pkg/math/libm` (the exponential, logarithmic, power and hyperbolic functions). Used so `^`, `exp`, `log`, and the hyperbolic functions return the correctly rounded results Neo4j's Java `Math` returns.

**License Text:**
```
MIT License

Copyright © 2005-2024 Rich Felker, et al.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Arm optimized-routines

**Source:** https://github.com/ARM-software/optimized-routines (vendored by musl)  
**License:** MIT License  
**Copyright:** Copyright (c) 2018, Arm Limited

**Description:** Correctly rounded `exp`, `exp2`, `log`, `log2`, and `pow` implementations ported to Go in `pkg/math/libm`. musl carries these files under the SPDX identifier `MIT`.

**License Text:**
```
MIT License

Copyright (c) 2018, Arm Limited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## JavaScript/TypeScript Dependencies

The following dependencies are used in the NornicDB UI (Bifrost):

### MIT Licensed

- **react** - UI library
- **react-dom** - React DOM rendering
- **react-router-dom** - Routing for React
- **zustand** - State management
- **lucide-react** - Icon library
- **vite** - Build tool
- **typescript** - TypeScript compiler
- **tailwindcss** - CSS framework
- **autoprefixer** - CSS post-processor
- **postcss** - CSS transformer

**Note:** All UI dependencies are MIT licensed and compatible with NornicDB's license.

---

## Summary of Non-MIT Licenses

| Component | License | Commercial Use | Modifications | Distribution |
|-----------|---------|----------------|---------------|--------------|
| qwen3-0.6b-Instruct | Apache 2.0 | ✅ Yes | ✅ Yes | ✅ Yes |
| BadgerDB | Apache 2.0 | ✅ Yes | ✅ Yes | ✅ Yes |
| Cobra CLI | Apache 2.0 | ✅ Yes | ✅ Yes | ✅ Yes |
| OpenTelemetry | Apache 2.0 | ✅ Yes | ✅ Yes | ✅ Yes |
| golang.org/x packages | BSD-3-Clause | ✅ Yes | ✅ Yes | ✅ Yes |
| YAML v3 | ISC | ✅ Yes | ✅ Yes | ✅ Yes |

**All dependencies are permissively licensed and allow commercial use, modification, and redistribution.**

---

## Attribution Requirements

When distributing NornicDB or derivative works:

1. **MIT Licensed Components** (NornicDB core, BGE-M3, llama.cpp, GGML): Include the MIT license text and copyright notices.

2. **Apache 2.0 Licensed Components** (Qwen2.5, BadgerDB, etc.): Include the Apache 2.0 license text and any NOTICE files.

3. **BSD Licensed Components** (Go standard libraries): Include BSD license text and copyright notices.

4. **Model Citations**: When using the bundled models in research or publications, cite the original model papers as specified by the authors.

---

## Compliance Notes

### For Users

- **Base Images (BYOM)**: Do not include bundled models. You are responsible for model licenses when mounting your own models.
- **Heimdall Images**: Include BGE-M3 (MIT) and Qwen2.5 (Apache 2.0). Both licenses permit commercial use.
- **From Source Builds**: Comply with licenses of dependencies listed in `go.mod` and `package.json`.

### For Distributors

If you redistribute NornicDB:

1. Include this `NOTICES.md` file
2. Include `LICENSE` file with NornicDB's MIT license
3. Retain all copyright notices in source code
4. For Docker images with models, include model license information in image metadata

### For Commercial Use

All licenses in NornicDB (MIT, Apache 2.0, BSD-3-Clause, ISC) explicitly permit commercial use without royalties. You are free to:

- Use NornicDB in commercial products
- Modify and create derivative works
- Redistribute original or modified versions
- Use bundled models in commercial applications

**Requirements:**
- Include license texts and copyright notices
- Provide attribution as specified by each license
- Do not use project or contributor names for endorsement without permission

---

## Updates and Maintenance

This notice file is maintained as part of the NornicDB project. When adding new dependencies or bundled models:

1. Add license information to this file
2. Verify license compatibility with MIT
3. Include attribution requirements
4. Update model download documentation

**Last Updated:** December 6, 2024  
**NornicDB Version:** 1.0.0

For questions about licensing, please open an issue at https://github.com/orneryd/NornicDB/issues
