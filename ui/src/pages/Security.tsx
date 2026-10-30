import { useState, useEffect, useId } from "react";
import { useNavigate } from "react-router-dom";
import { PageLayout } from "../components/common/PageLayout";
import { PageHeader } from "../components/common/PageHeader";
import { FormInput } from "../components/common/FormInput";
import { Button } from "../components/common/Button";
import { Alert } from "../components/common/Alert";
import { Copy, Check } from "lucide-react";
import { BASE_PATH, joinBasePath } from "../utils/basePath";

interface GeneratedToken {
  token: string;
  subject: string;
  expires_at?: string;
  expires_in?: number;
  roles: string[];
}

interface UserInfo {
  id: string;
  username: string;
  email?: string;
  roles: string[];
  auth_method?: string;
  oauth_provider?: string;
}

export function Security() {
  const navigate = useNavigate();
  const emailId = useId();
  const oldPasswordId = useId();
  const newPasswordId = useId();
  const confirmPasswordId = useId();
  const tokenSubjectId = useId();
  const customExpiryId = useId();
  const [subject, setSubject] = useState("");
  const [expiresIn, setExpiresIn] = useState("30d");
  const [customExpiry, setCustomExpiry] = useState("");
  const [generatedToken, setGeneratedToken] = useState<GeneratedToken | null>(
    null,
  );
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [isAdmin, setIsAdmin] = useState(false);
  const [checkingAuth, setCheckingAuth] = useState(true);
  const [userInfo, setUserInfo] = useState<UserInfo | null>(null);

  // Password change state
  const [oldPassword, setOldPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [passwordError, setPasswordError] = useState("");
  const [passwordSuccess, setPasswordSuccess] = useState(false);
  const [changingPassword, setChangingPassword] = useState(false);

  // Profile update state
  const [email, setEmail] = useState("");
  const [profileError, setProfileError] = useState("");
  const [profileSuccess, setProfileSuccess] = useState(false);
  const [updatingProfile, setUpdatingProfile] = useState(false);

  useEffect(() => {
    // Check if user is admin and get user info
    fetch(joinBasePath(BASE_PATH, "/auth/me"), {
      credentials: "include",
    })
      .then((res) => res.json())
      .then((data) => {
        const roles = data.roles || [];
        setIsAdmin(roles.includes("admin"));
        setUserInfo(data);
        setEmail(data.email || "");
        setCheckingAuth(false);
      })
      .catch(() => {
        setCheckingAuth(false);
        navigate("/login");
      });
  }, [navigate]);

  const handleGenerate = async () => {
    setError("");
    setIsLoading(true);
    setGeneratedToken(null);

    const expiry = expiresIn === "custom" ? customExpiry : expiresIn;

    try {
      const response = await fetch(joinBasePath(BASE_PATH, "/auth/api-token"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        credentials: "include",
        body: JSON.stringify({
          subject: subject || "api-token",
          expires_in: expiry === "never" ? "0" : expiry,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || "Failed to generate token");
      }

      const data = await response.json();
      setGeneratedToken(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate token");
    } finally {
      setIsLoading(false);
    }
  };

  const copyToClipboard = () => {
    if (generatedToken) {
      navigator.clipboard.writeText(generatedToken.token);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  if (checkingAuth) {
    return (
      <PageLayout>
        <div className="flex items-center justify-center flex-1">
          <div className="w-12 h-12 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
        </div>
      </PageLayout>
    );
  }

  return (
    <PageLayout>
      <PageHeader
        title="Security & API Tokens"
        backTo="/"
        actions={
          isAdmin && (
            <div className="flex gap-2">
              <Button
                variant="secondary"
                onClick={() => navigate("/security/database-access")}
              >
                Database Access
              </Button>
              <Button
                variant="secondary"
                onClick={() => navigate("/security/retention")}
              >
                Retention Policies
              </Button>
              <Button
                variant="secondary"
                onClick={() => navigate("/security/lifecycle")}
              >
                MVCC Lifecycle
              </Button>
              <Button
                variant="secondary"
                onClick={() => navigate("/security/knowledge-policies")}
              >
                Knowledge Policies
              </Button>
              <Button
                variant="secondary"
                onClick={() => navigate("/security/admin")}
              >
                👥 Admin Panel
              </Button>
            </div>
          )
        }
      />

      {/* Main Content */}
      <main className="max-w-4xl mx-auto p-6">
        {/* Authentication Info */}
        {userInfo && (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg p-4 mb-6">
            <h2 className="text-sm font-semibold text-norse-silver mb-2">
              Authentication Method
            </h2>
            <div className="flex items-center gap-2">
              {userInfo.auth_method === "oauth" ? (
                <>
                  <span className="text-green-400">🔐 OAuth</span>
                  {userInfo.oauth_provider && (
                    <span className="text-norse-fog text-sm">
                      ({userInfo.oauth_provider})
                    </span>
                  )}
                  <span className="text-norse-fog text-sm ml-auto">
                    Your account is managed by the OAuth provider. You can
                    generate NornicDB API tokens below for programmatic access.
                  </span>
                </>
              ) : (
                <>
                  <span className="text-blue-400">🔑 Password</span>
                  <span className="text-norse-fog text-sm ml-auto">
                    Your account uses password authentication. You can generate
                    API tokens for programmatic access.
                  </span>
                </>
              )}
            </div>
          </div>
        )}

        {/* Profile Update Section */}
        {userInfo && userInfo.auth_method !== "oauth" && (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 mb-8">
            <h2 className="text-lg font-semibold text-white mb-4">
              Profile Settings
            </h2>

            <div className="space-y-4">
              <FormInput
                id={emailId}
                type="email"
                label="Email Address"
                value={email}
                onChange={setEmail}
                placeholder="your.email@example.com"
              />

              <Button
                onClick={async () => {
                  setProfileError("");
                  setProfileSuccess(false);
                  setUpdatingProfile(true);

                  try {
                    const response = await fetch(
                      joinBasePath(BASE_PATH, "/auth/profile"),
                      {
                        method: "PUT",
                        headers: {
                          "Content-Type": "application/json",
                        },
                        credentials: "include",
                        body: JSON.stringify({ email }),
                      },
                    );

                    if (!response.ok) {
                      const data = await response.json();
                      throw new Error(
                        data.message || "Failed to update profile",
                      );
                    }

                    setProfileSuccess(true);
                    setTimeout(() => setProfileSuccess(false), 3000);

                    // Refresh user info
                    const meResponse = await fetch(
                      joinBasePath(BASE_PATH, "/auth/me"),
                      {
                        credentials: "include",
                      },
                    );
                    const meData = await meResponse.json();
                    setUserInfo(meData);
                    setEmail(meData.email || "");
                  } catch (err) {
                    setProfileError(
                      err instanceof Error
                        ? err.message
                        : "Failed to update profile",
                    );
                  } finally {
                    setUpdatingProfile(false);
                  }
                }}
                disabled={updatingProfile}
                loading={updatingProfile}
              >
                Update Profile
              </Button>

              {profileError && <Alert type="error" message={profileError} />}
              {profileSuccess && (
                <Alert type="success" message="Profile updated successfully!" />
              )}
            </div>
          </div>
        )}

        {/* Password Change Section */}
        {userInfo && userInfo.auth_method !== "oauth" && (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 mb-8">
            <h2 className="text-lg font-semibold text-white mb-4">
              Change Password
            </h2>

            <div className="space-y-4">
              <FormInput
                id={oldPasswordId}
                type="password"
                label="Current Password"
                value={oldPassword}
                onChange={setOldPassword}
              />

              <FormInput
                id={newPasswordId}
                type="password"
                label="New Password"
                value={newPassword}
                onChange={setNewPassword}
              />

              <FormInput
                id={confirmPasswordId}
                type="password"
                label="Confirm New Password"
                value={confirmPassword}
                onChange={setConfirmPassword}
              />

              <Button
                onClick={async () => {
                  setPasswordError("");
                  setPasswordSuccess(false);

                  if (newPassword !== confirmPassword) {
                    setPasswordError("New passwords do not match");
                    return;
                  }

                  if (newPassword.length < 8) {
                    setPasswordError(
                      "New password must be at least 8 characters",
                    );
                    return;
                  }

                  setChangingPassword(true);

                  try {
                    const response = await fetch(
                      joinBasePath(BASE_PATH, "/auth/password"),
                      {
                        method: "POST",
                        headers: {
                          "Content-Type": "application/json",
                        },
                        credentials: "include",
                        body: JSON.stringify({
                          old_password: oldPassword,
                          new_password: newPassword,
                        }),
                      },
                    );

                    if (!response.ok) {
                      const data = await response.json();
                      throw new Error(
                        data.message || "Failed to change password",
                      );
                    }

                    setPasswordSuccess(true);
                    setOldPassword("");
                    setNewPassword("");
                    setConfirmPassword("");
                    setTimeout(() => setPasswordSuccess(false), 3000);
                  } catch (err) {
                    setPasswordError(
                      err instanceof Error
                        ? err.message
                        : "Failed to change password",
                    );
                  } finally {
                    setChangingPassword(false);
                  }
                }}
                disabled={
                  changingPassword ||
                  !oldPassword ||
                  !newPassword ||
                  !confirmPassword
                }
                loading={changingPassword}
                className="w-full"
              >
                Change Password
              </Button>

              {passwordError && <Alert type="error" message={passwordError} />}
              {passwordSuccess && (
                <Alert
                  type="success"
                  message="Password changed successfully!"
                />
              )}
            </div>
          </div>
        )}

        {/* Info Banner */}
        <Alert
          type="info"
          title="About API Tokens"
          message="API tokens are stateless JWT tokens that can be used for MCP server configurations and other API integrations. These tokens inherit your current roles and permissions. Tokens are not stored — once generated, save them securely as they cannot be retrieved later."
          className="mb-8"
        />

        {/* Token Generator - Admin Only */}
        {isAdmin && (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 mb-8">
            <h2 className="text-lg font-semibold text-white mb-4">
              Generate API Token
            </h2>

            <div className="space-y-4">
              <FormInput
                id={tokenSubjectId}
                label="Token Label (Subject)"
                value={subject}
                onChange={setSubject}
                placeholder="e.g., my-mcp-server, prod-api, cursor-agent"
              />
              <p className="text-xs text-norse-fog -mt-2">
                A descriptive label to help you identify this token later
              </p>

              {/* Expiration */}
              <div>
                <span className="block text-sm text-norse-silver mb-2">
                  Token Expiration
                </span>
                <div className="flex gap-2 flex-wrap">
                  {[
                    "1h",
                    "24h",
                    "7d",
                    "30d",
                    "90d",
                    "365d",
                    "never",
                    "custom",
                  ].map((option) => (
                    <button
                      type="button"
                      key={option}
                      onClick={() => setExpiresIn(option)}
                      className={`px-3 py-1.5 rounded text-sm transition-colors ${
                        expiresIn === option
                          ? "bg-nornic-primary text-white"
                          : "bg-norse-stone text-norse-silver hover:bg-norse-rune"
                      }`}
                    >
                      {option === "never"
                        ? "Never"
                        : option === "custom"
                          ? "Custom"
                          : option}
                    </button>
                  ))}
                </div>
                {expiresIn === "custom" && (
                  <FormInput
                    id={customExpiryId}
                    value={customExpiry}
                    onChange={setCustomExpiry}
                    placeholder="e.g., 48h, 14d, 6mo"
                    className="mt-2"
                  />
                )}
              </div>

              <Button
                onClick={handleGenerate}
                disabled={isLoading}
                loading={isLoading}
                variant="success"
                className="w-full"
              >
                Generate Token
              </Button>

              {error && <Alert type="error" message={error} />}
            </div>
          </div>
        )}

        {/* Generated Token Display */}
        {generatedToken && (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 mb-8">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-green-400">
                ✓ Token Generated
              </h2>
              <span className="text-xs text-norse-fog">
                {generatedToken.expires_at
                  ? `Expires: ${new Date(generatedToken.expires_at).toLocaleString()}`
                  : "Never expires"}
              </span>
            </div>

            <div className="bg-norse-stone rounded p-4 mb-4">
              <div className="flex items-start justify-between gap-4">
                <code className="text-sm text-green-300 break-all flex-1 font-mono">
                  {generatedToken.token}
                </code>
                <Button
                  variant={copied ? "success" : "secondary"}
                  size="sm"
                  onClick={copyToClipboard}
                  icon={copied ? Check : Copy}
                >
                  {copied ? "Copied!" : "Copy"}
                </Button>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 text-sm mb-4">
              <div>
                <span className="text-norse-fog">Subject:</span>
                <span className="text-white ml-2">
                  {generatedToken.subject}
                </span>
              </div>
              <div>
                <span className="text-norse-fog">Roles:</span>
                <span className="text-white ml-2">
                  {generatedToken.roles.join(", ")}
                </span>
              </div>
            </div>

            {/* Usage Example */}
            <div className="mt-4 pt-4 border-t border-norse-rune">
              <h3 className="text-sm font-semibold text-norse-silver mb-2">
                Usage Example (Claude Desktop / MCP Config)
              </h3>
              <pre className="bg-norse-stone rounded p-3 text-xs overflow-x-auto break-words whitespace-pre-wrap">
                <code className="text-norse-silver">{`{
  "mcpServers": {
    "nornicdb": {
      "url": "http://127.0.0.1:7474/mcp",
      "name": "Knowledge Graph TODO MCP Server",
      "description": "MCP server for TODO tracking with Graph-RAG memory system",
      "headers": {
        "Authorization": "Bearer ${generatedToken.token.substring(0, 40)}..."
      }
    }
  }
}`}</code>
              </pre>
              <p className="text-xs text-norse-fog mt-2">
                For Claude Desktop: Add this to your{" "}
                <code className="text-norse-silver">
                  ~/Library/Application
                  Support/Claude/claude_desktop_config.json
                </code>
              </p>
            </div>
          </div>
        )}

        {/* Security Tips */}
        <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6">
          <h2 className="text-lg font-semibold text-white mb-4">
            🔐 Security Best Practices
          </h2>
          <ul className="space-y-2 text-sm text-norse-silver">
            <li className="flex gap-2">
              <span className="text-valhalla-gold">•</span>
              <span>
                Use descriptive labels to track which token is used where
              </span>
            </li>
            <li className="flex gap-2">
              <span className="text-valhalla-gold">•</span>
              <span>
                Set appropriate expiration times — shorter is more secure
              </span>
            </li>
            <li className="flex gap-2">
              <span className="text-valhalla-gold">•</span>
              <span>
                Store tokens securely (environment variables, secrets managers)
              </span>
            </li>
            <li className="flex gap-2">
              <span className="text-valhalla-gold">•</span>
              <span>Never commit tokens to version control</span>
            </li>
            <li className="flex gap-2">
              <span className="text-valhalla-gold">•</span>
              <span>
                Rotate tokens periodically, especially for long-lived
                integrations
              </span>
            </li>
          </ul>
        </div>
      </main>
    </PageLayout>
  );
}
